package engine

// This file carries the engine's shared path and baseline helpers used across the
// reconcile, outbound, conflict and localreconcile paths: markSynced (the single place
// an applied op records that local and server agree) and the path-containment guards
// abs/noSymlinkEscape/splitComponents that map an untrusted view-relative path to a
// safe on-disk location under the sync root.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/store"
)

// bestEffortSyncDirs flushes the directory entries an already-applied replay branch is
// about to ack without touching disk itself.
//
// fsutil.Rename/Mkdir make their own mutations durable, but a crash (or a failed flush)
// between the on-disk mutation and the baseline write leaves a replay that finds the work
// already done - applyRename's gone-source/present-destination swallow, and
// relocateTrackedDirForMkdir's old-path-gone/destination-present branch - and those
// branches previously acked with the entry still non-durable, because the ONLY flush of
// that entry lived in the call that already returned (the same
// one-shot class mkdirAllDurable closes for the create axis). Re-flushing here gives the
// rename axis the same retry guarantee.
//
// It is best-effort by design: the disk state is already correct and re-running the op
// would not improve it, so a flush failure is logged, not returned - surfacing it would
// dead-end the op in an eternal transient retry that no later write can clear, and the
// periodic scan re-detects any entry a crash does lose. Duplicate and missing directories
// are skipped (an idempotent replay may have removed the source's parent entirely).
func (e *Engine) bestEffortSyncDirs(dirs ...string) {
	var done []string
	for _, dir := range dirs {
		if slices.Contains(done, dir) {
			continue
		}
		done = append(done, dir)
		if err := e.fsSyncDir(dir); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			e.log.Warn("engine: could not flush directory entry on an already-applied replay",
				"dir", dir, "err", err)
		}
	}
}

// warnCorruptBaseline logs that a stored baseline record no longer decodes and is being
// treated as absent. bbolt checksums only its meta pages, so a latent value corruption
// surfaces at read time as store.ErrCorruptRecord (files.GetBaseline). The reconcile read
// paths must NOT propagate it as a fatal error: doing so aborts the whole batch without
// advancing the cursor, so the same page is re-pulled and re-fails every cycle forever, and
// the targeting Put/Delete that would heal the key never runs. Treating
// the record as absent lets the loop proceed and the tolerant healing write (PutBaseline /
// DeleteBaseline) clear the key; the direction is fail-safe (an absent
// synced base funnels toward preserve, never clobber). One Warn per read makes the corruption
// observable rather than silent.
//
// The same rule extends to the APPLY/CLEANUP read set (the reconcile read
// set alone was not enough: a corrupt record at a move's source, or at a descendant of a
// directory being removed, made the strict apply-side GetBaseline fail the op transiently
// every drain forever, and the healing write below it never ran). The unified rule every
// baseline read on the apply/cleanup side follows:
//
//   - An ErrCorruptRecord read is NEVER surfaced as a plain error (which the executor
//     classifies transient - an eternal retry no later write can clear).
//   - The record is treated as absent/untracked in whichever direction is NON-destructive
//     for that site: divergence gates preserve (a corrupt base can never prove content in
//     sync, so on-disk content is set aside whole, mirroring the reconcile capture gates);
//     classification reads fall back to their absent branch.
//   - A cleanup helper whose natural action is a tolerant Delete/Put completes that healing
//     write (DeleteBaseline deletes an undecodable value anyway; putBaselineTx overwrites
//     one), so the corrupt key is cleared rather than fossilised; a site that can rebuild
//     the record from the op's own facts overwrites it with those.
func (e *Engine) warnCorruptBaseline(where string, viewID uint64, path string, err error) {
	e.log.Warn("engine: baseline record does not decode; treating as absent (a healing write will clear it)",
		"where", where, "view", viewID, "path", path, "err", err)
}

// contentDiverged reports whether two content hashes PROVE the server no longer holds the
// version a base was recorded against.
//
// The single definition of a predicate the destructive preconditions all need: the upload guard
// (H1), the remove precondition's target facts, and the vanished-source classifier each had
// their own copy of `a != "" && b != "" && a != b`. They agreed, but the doctrine they encode is
// too load-bearing to keep in three places and reason about separately - an empty hash on either
// side is NOT agreement and NOT divergence, it is an absence of evidence, and each call site has
// its own (documented) degradation for that third state. Spelling the proof once leaves each
// site with only its own degradation to explain.
//
// False for an empty operand is therefore deliberate, and is why this cannot be written as an
// inequality: it is "proved different", never "not proved equal".
func contentDiverged(serverHash, baseHash string) bool {
	return serverHash != "" && baseHash != "" && serverHash != baseHash
}

// markSynced loads the baseline at view_id\0path (treating an absent record as a
// fresh one), applies mutate, forces State to StateSynced and writes it back. It is
// the single place the executor records that an applied op brought local and server
// into agreement. A corrupt (undecodable) prior record is treated as fresh too, so the
// PutBaseline below overwrites and heals it rather than the read wedging the apply forever
// (mirroring the isNotFound branch).
//
// Recording agreement at a path is also the strongest possible refutation of an absence
// streak for it (absencestreak.go): a node this client just downloaded, created or adopted
// demonstrably occupies the path, so any run of path-errors remembered for it is stale and
// must not survive to corroborate a LATER absence on one further probe. A streak that reached
// its verdict is deliberately not cleared at the verdict (the ancestor walk needs every rung to
// stay settled in the same pass), so without this reset a path whose absence converged and was
// then re-created while a settled verdict still stands would let a single conflated 28672 ack a
// delete the server never performed - the very hole the corroboration exists to close.
func (e *Engine) markSynced(viewID uint64, path string, mutate func(*store.Baseline)) error {
	b, err := e.store.GetBaseline(viewID, path)
	switch {
	case isNotFound(err):
		b = store.Baseline{}
	case errors.Is(err, store.ErrCorruptRecord):
		e.warnCorruptBaseline("mark-synced", viewID, path, err)
		b = store.Baseline{}
	case err != nil:
		return fmt.Errorf("load baseline %d/%q: %w", viewID, path, err)
	}
	mutate(&b)
	b.State = store.StateSynced
	if err := e.store.PutBaseline(viewID, path, b); err != nil {
		return fmt.Errorf("write baseline %d/%q: %w", viewID, path, err)
	}
	e.clearAbsentProbe(int64(viewID), path) // #nosec G115 -- a view_id is a non-negative identifier
	return nil
}

// abs maps a view-relative path to its absolute on-disk location under the sync
// root. Each view is materialised in a subdirectory named after the view (falling back
// to the numeric view id when the name is unknown). Distinct view names give distinct
// directories; two views sharing a name resolve to the same directory, which the
// drain's scheduler keys on (see viewDir/serializeKeys) so their ops serialise rather
// than racing on the same files.
//
// Both the view name and the relative path arrive from the server and are treated as
// untrusted. Each is validated for containment on its own - before being joined - so
// neither can redirect a write outside the sync root: filepath.IsLocal rejects an
// absolute path, the empty path and any ".." component, so a malicious view name like
// "../outside" or a path like "../Other/f" is refused, and an explicit "." check on each
// rejects a name or path that collapses to the view root itself (a view named "." or
// "a/.." would otherwise resolve to the sync root and clobber another view). Beyond the
// lexical checks,
// noSymlinkEscape walks the existing path components and refuses any symlink, since
// filepath.IsLocal ignores symlinks and the engine never creates them - a symlink in
// the tree could only redirect an apply outside the sync root.
func (e *Engine) abs(viewID uint64, relPath string) (string, error) {
	v, err := e.store.GetView(viewID)
	if err != nil {
		return "", fmt.Errorf("resolve view %d: %w", viewID, err)
	}
	name := v.Name
	switch {
	case e.views.Flat:
		// A flat layout resolves the view to the sync root itself, so there is no
		// directory component and none of the name checks below apply: the view's
		// name is never spelled on disk. Only one view can be flat
		// (config.Views.Validate), so no two views can collapse onto one path.
		name = ""
	case name == "":
		name = strconv.FormatUint(viewID, 10)
	default:
		// The server uses slash-separated paths; normalise to the OS separator so a
		// multi-segment name resolves and is symlink-walked component-by-component (see
		// noSymlinkEscape) rather than as one opaque element.
		name = filepath.FromSlash(name)
		if !hostRepresentablePath(name) {
			// A view name this host would silently rewrite rather than materialise verbatim
			// (on Windows a component ending in a dot or a space; namemangle.go). The view's
			// whole subtree would land in a directory spelled differently from every baseline
			// key describing it, so refuse the name outright - the op dead-letters with a trace
			// instead of syncing a view into a directory nobody can address.
			return "", fmt.Errorf("%w: view %d name %q cannot be represented verbatim on this host",
				errRejectedPath, viewID, name)
		}
		if !filepath.IsLocal(name) || filepath.Clean(name) == "." || strings.IndexByte(name, 0) >= 0 {
			// filepath.IsLocal only guarantees containment - it accepts "." and any name
			// that cleans to it (e.g. "a/.."), which would resolve the view to the sync root
			// itself and let it remove or overwrite another view's directory, and it accepts
			// an embedded NUL byte on Unix (os.Lstat would then return EINVAL, not
			// IsNotExist, so the op would wedge the queue rather than dead-letter). Reject all
			// of those so every view name maps to a proper, isolated subdirectory.
			return "", fmt.Errorf("%w: view %d name %q does not map to a subdirectory", errRejectedPath, viewID, name)
		}
	}
	rel := filepath.FromSlash(relPath)
	if !filepath.IsLocal(rel) || filepath.Clean(rel) == "." || strings.IndexByte(rel, 0) >= 0 {
		return "", fmt.Errorf("%w: path %q in view %d escapes the view root", errRejectedPath, relPath, viewID)
	}
	if !hostRepresentablePath(rel) {
		// Defence in depth for the name-mangling class reconcile's containable() screens out
		// (namemangle.go): a component this host would silently rewrite - on Windows one ending
		// in a dot or a space - resolves to a DIFFERENT on-disk node than the one addressed, so
		// every apply built on this path would mis-apply while reporting success. An op that
		// predates the screen (queued by an older build, or by cold start) reaches here instead,
		// and errRejectedPath dead-letters it with a trace rather than letting it land.
		return "", fmt.Errorf("%w: path %q in view %d cannot be represented verbatim on this host",
			errRejectedPath, relPath, viewID)
	}
	if err := e.noSymlinkEscape(name, rel); err != nil {
		return "", err
	}
	return filepath.Join(e.syncRoot, name, rel), nil
}

// noSymlinkEscape walks the on-disk components of syncRoot/name/rel and refuses the
// path if any existing component redirects elsewhere - a symlink on every host, and on
// Windows a junction too (pathRedirects; a mode-bit-only test
// is blind to the one redirection an UNPRIVILEGED Windows user can create).
// Lexical containment (filepath.IsLocal)
// cannot see through either, so a pre-existing redirection such as Photos/link → outside
// would let a remote download to link/file write outside the sync root. The engine
// never creates one, so any found along the path can only be an escape
// vector. Components that do not yet exist end the walk: there is nothing to traverse.
// The sync root itself is not inspected - reaching it through a symlink is the
// operator's own choice and stays contained.
func (e *Engine) noSymlinkEscape(name, rel string) error {
	return e.noSymlinkEscapeMemo(name, rel, nil)
}

// symlinkMemo is the set of absolute path prefixes a single scan pass has already Lstat'ed
// and proven to be existing non-symlink components. A nil map disables memoization.
//
// scanView calls noSymlinkEscape for EVERY entry it surfaces, and each call re-walks the
// entry's whole ancestor chain: O(entries × depth) Lstat syscalls every scan interval, with
// the shallow directories of a deep tree re-stat'ed thousands of times per pass.
// WalkDir is parents-first, so a directory's own check has already proven every
// ancestor by the time its children are visited - the memo turns the repeat proof into a map
// hit and leaves ~one Lstat per NEW component per pass.
//
// A cached proof may NOT stand in for the checked path's immediate parent, and
// noSymlinkEscapeMemo therefore always Lstats it afresh. WalkDir proves a directory in its
// OWN callback and only then calls ReadDir on it, so a directory swapped for a symlink in
// that window is enumerated through the link: with the parent served from the memo, every
// child of the link TARGET would pass the guard and be surfaced as local content - foreign
// directory names pushed to the server as remote mkdirs (file bytes stay protected, since
// openUploadSource re-opens no-follow component by component). That is exactly the case the
// scanner header calls out - "every child of a tracked subdirectory swapped for a directory
// symlink between the parent's readdir and the child's callback" - so the memo must not
// weaken it. Re-proving just the parent keeps the cost at O(entries), not
// O(entries × depth). scanView additionally drops the whole memo whenever the walk moves to
// another directory, so the first entry of each directory re-proves the FULL chain.
//
// Only positive results are memoized, and only within one pass (the map is created per
// scanView call and dropped with it), so a symlink swapped in during the pass is caught by
// the next one at the latest - that pass Lstats every component afresh at its own entry's
// callback. The residual window (a STRICT ancestor of the parent swapped after the walk
// descended past it and while it is still listing the same directory) is the same TOCTOU
// class the un-memoized walk already had between its Lstat and the op that follows - the
// check is a guard against a PRE-EXISTING symlink, not a lock. A component that does not
// exist is never memoized: it ends the walk, and a later entry under it must re-observe it.
type symlinkMemo map[string]bool

// noSymlinkEscapeMemo is noSymlinkEscape with an optional per-pass memo of prefixes already
// proven symlink-free (see symlinkMemo). Verdicts are identical to the un-memoized walk for
// a tree that does not change under it; only the number of Lstat calls differs.
func (e *Engine) noSymlinkEscapeMemo(name, rel string, memo symlinkMemo) error {
	// Split the view name into its own components too: a multi-segment name (e.g.
	// "a/b") materialises as nested directories, and every intermediate one must be
	// Lstat-checked for a symlink, not just the final element.
	comps := append(splitComponents(name), splitComponents(rel)...)
	// The memo may only stand in for the STRICT ancestors of the path's immediate parent:
	// the final component (the entry itself) and the parent it hangs off are always
	// Lstat'ed afresh - see symlinkMemo for the swapped-before-descent case that depends
	// on it.
	memoDepth := len(comps) - 2
	cur := e.syncRoot
	for i, c := range comps {
		cur = filepath.Join(cur, c)
		if i < memoDepth && memo[cur] {
			continue // already proven an existing, non-symlink component this pass
		}
		fi, err := e.lstatPath(cur)
		if err != nil {
			// A prior proof of this component no longer holds; drop it so a deeper path
			// checked later in the pass cannot be served the stale entry (delete on a nil
			// map is a no-op).
			delete(memo, cur)
			if os.IsNotExist(err) {
				return nil // remaining components do not exist yet
			}
			return fmt.Errorf("engine: stat %q: %w", cur, err)
		}
		if pathRedirects(cur, fi.Mode()) {
			// The engine never creates a symlink or a junction, so a redirecting component
			// here is permanent: it cannot be applied and never will be without operator
			// intervention. Mark it with errRejectedPath so applyOp dead-letters the op
			// instead of re-failing it every drain - an undropped op here would also fail
			// syncView's trailing drain and abort the poll before the remaining changed views
			// sync, starving them.
			delete(memo, cur) // a swapped-in redirection must never stay cached as proven clean
			return fmt.Errorf("%w: path component %q redirects elsewhere (symlink or junction); refusing to apply outside the sync root", errRejectedPath, cur)
		}
		if memo != nil {
			if fi.IsDir() {
				// Only a directory can be an ancestor of a later path, so memoizing just
				// directories keeps the memo O(dirs) rather than O(entries) - a leaf file's
				// entry could never be hit again anyway.
				memo[cur] = true
			} else {
				// A directory proven earlier in the pass is now a non-directory: it can no
				// longer be anyone's ancestor, so evict the stale proof.
				delete(memo, cur)
			}
		}
	}
	return nil
}

// onDiskAbsNoSymlink resolves the canonical NFC path abs to its real on-disk spelling
// (onDiskAbs) and, when the NFC-tolerant fallback selected a path other than the literal one
// e.abs already symlink-checked, re-validates that the resolved path has no symlink component.
//
// e.abs runs noSymlinkEscape over the LITERAL NFC path, but that walk stops at the first
// component that does not exist (returning nil) - and on a byte-preserving filesystem the NFC
// path often does not exist because the name is stored as NFD. resolveNFCPath then matches
// directory entries by their normalized name and can walk through, or select, a symlinked
// component the literal-path check never inspected. A destructive op (os.RemoveAll / os.Rename)
// applied to such a path would follow that symlink and could operate outside the sync root.
// Returning errRejectedPath makes the caller dead-letter the op like any other unappliable
// path rather than apply a destructive op through a symlink.
//
// When the resolution did not change the path (the common NFC case, or no normalize-equivalent
// entry) the literal path was already symlink-checked by e.abs, so no re-check is needed.
func (e *Engine) onDiskAbsNoSymlink(abs string) (string, error) {
	return e.recheckResolvedSymlink(abs, e.onDiskAbs(abs))
}

// onDiskAbsPrefix resolves abs's longest existing on-disk prefix to its real spelling
// (resolveNFCPathPrefix), keeping not-yet-existing trailing components literal (NFC); when
// nothing resolves differently the input is returned unchanged, mirroring onDiskAbs.
func (e *Engine) onDiskAbsPrefix(abs string) string {
	rel, err := filepath.Rel(e.syncRoot, abs)
	if err != nil {
		return abs
	}
	if realRel, ok := e.resolveNFCPathPrefix(rel); ok && realRel != rel {
		return filepath.Join(e.syncRoot, realRel)
	}
	return abs
}

// onDiskAbsPrefixNoSymlink is onDiskAbsNoSymlink's materialising counterpart: it resolves abs
// through onDiskAbsPrefix - the longest existing on-disk prefix, with not-yet-existing
// components kept literal (NFC) - and re-validates a changed resolution for symlink
// components. The apply paths that CREATE the node at abs (the download promote and mkdir)
// use it so an existing NFD-spelled target or parent directory is reused
// rather than shadowed by a parallel NFC twin entry, while a fresh create still materialises
// under the canonical NFC name.
//
// It is also where the materialising verbs get the 8.3 short-name alias gate (shortname.go):
// a component that resolves to a DIFFERENTLY-named on-disk entity is not a
// path this client can create at, so it is refused as errRejectedPath here - before
// untrackedFilePresent hashes the stranger's content and the never-lose funnel carries that
// innocent node off to a _Conflict copy.
func (e *Engine) onDiskAbsPrefixNoSymlink(abs string) (string, error) {
	resolved, err := e.recheckResolvedSymlink(abs, e.onDiskAbsPrefix(abs))
	if err != nil {
		return "", err
	}
	if err := e.rejectShortNameAlias(resolved); err != nil {
		return "", err
	}
	return resolved, nil
}

// recheckResolvedSymlink re-validates an NFC-tolerant path resolution: when the resolution
// selected a path other than the literal one e.abs already symlink-checked, the resolved
// spelling is walked for symlink components (pathHasSymlinkComponent) - components that do
// not exist yet end that walk, so a partial (prefix) resolution's literal tail is trivially
// clean - and a hit is errRejectedPath so the caller dead-letters the op rather than apply a
// destructive or materialising op through a symlink.
func (e *Engine) recheckResolvedSymlink(abs, resolved string) (string, error) {
	if resolved != abs && e.pathHasSymlinkComponent(resolved) {
		return "", fmt.Errorf("%w: resolved on-disk path %q has a symlink component", errRejectedPath, resolved)
	}
	return resolved, nil
}

// lstatPath is the no-follow stat the symlink-component walk uses, routed through the
// e.lstat seam when one is wired (see Engine.lstat) and os.Lstat otherwise, so an Engine
// built as a literal in a test behaves exactly like a production one.
func (e *Engine) lstatPath(name string) (os.FileInfo, error) {
	if e.lstat != nil {
		return e.lstat(name)
	}
	return os.Lstat(name)
}

// listDir is the directory listing resolveNFCComponents' NFD fallback uses, routed
// through the e.readDir seam when one is wired (see Engine.readDir) and os.ReadDir
// otherwise, so an Engine built as a literal in a test behaves exactly like a
// production one instead of nil-panicking on the first non-ASCII component.
func (e *Engine) listDir(name string) ([]os.DirEntry, error) {
	if e.readDir != nil {
		return e.readDir(name)
	}
	return os.ReadDir(name)
}

// splitComponents splits an OS-separated relative path into its non-empty
// components, in order.
func splitComponents(rel string) []string {
	parts := strings.Split(rel, string(filepath.Separator))
	out := parts[:0]
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// The apply path's filesystem seams, each falling back to the real implementation when the
// field is nil so an Engine built literally in a test needs no wiring. Their purpose and
// the tests that use them are described on the fields in engine.go.

func (e *Engine) fsRenameNoReplace(oldpath, newpath string) error {
	if e.renameNoReplace != nil {
		return e.renameNoReplace(oldpath, newpath)
	}
	return fsutil.RenameNoReplace(oldpath, newpath)
}

func (e *Engine) fsConflictCopyLstat(name string) (os.FileInfo, error) {
	if e.conflictCopyLstat != nil {
		return e.conflictCopyLstat(name)
	}
	return os.Lstat(name)
}

func (e *Engine) fsSyncDir(dir string) error {
	if e.syncDir != nil {
		return e.syncDir(dir)
	}
	return fsutil.SyncDir(dir)
}

func (e *Engine) fsCreateStagingTemp(dir, pattern string) (*os.File, error) {
	if e.createStagingTemp != nil {
		return e.createStagingTemp(dir, pattern)
	}
	return os.CreateTemp(dir, pattern)
}

func (e *Engine) fsSetMeta(path string, mtime int64, unixPerm uint32) error {
	if e.setMeta != nil {
		return e.setMeta(path, mtime, unixPerm)
	}
	return fsutil.SetMeta(path, mtime, unixPerm)
}
