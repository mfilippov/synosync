package engine

// This file carries the Phase-6c local filesystem scanner: the read-only walk of a
// view's on-disk directory that the local-reconcile pass (localreconcile.go) diffs
// against the stored baselines to detect local creates/modifies/deletes. It performs
// no store mutation and no network I/O - it only observes disk.
//
// MD4 content hashing is streamed incrementally so a large file is never buffered
// whole (docs/INVARIANTS.md#streaming). The same containment and
// no-symlink-escape guards the inbound executor applies (containable, noSymlinkEscape)
// are reused here so a path that escapes the sync root, or sits behind a pre-existing
// symlink, is never surfaced as a local change. The executor's own staging temp files
// (".<base>.dl-<digits>") are skipped so they are never mistaken for user content to push;
// the name is matched precisely (isStagingName) so a real user file that merely resembles
// the shape still syncs. Keep-both conflict copies are NOT name-filtered here: once Phase 6d
// pushes them they are ordinary server files, so the scanner treats them as real user content
// and the metadata-backed identification (the DESIGN §13 `conflicts` list) is the source of
// conflict provenance - a name match could only misfire on a legitimate user file.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// scanEntry is one observed on-disk node under a view's directory: a regular file or a
// directory. relPath is the slash-separated, view-relative path (the same form the store
// keys baselines under and the wire uses). For a directory md4 is empty and size is 0.
type scanEntry struct {
	relPath  string // slash-separated, view-relative
	isDir    bool
	size     int64
	mtime    int64  // modification time, Unix seconds
	unixPerm uint32 // permission bits in the wire-decimal form (octal read as base-10, e.g. 0o755 -> 755)
	md4      string // lowercase-hex MD4 of the content; empty for directories
	// dirID is a directory's (device, inode) when the platform reports one (hasDirID); the
	// nested-unmount guard compares it across scans (emptieddir.go).
	dirID    fileIdent
	hasDirID bool
}

// fileIdent identifies a filesystem object by device and inode.
type fileIdent struct{ dev, ino uint64 }

// scanCtxCheckEvery is how many walk entries scanView (and the subtree divergence sweep) may
// process between context checks. The check runs on the FIRST entry of
// every batch, so an already-cancelled context aborts before any entry is processed rather than
// after a batch of them. It trades shutdown latency against the per-entry cost of the check: 64
// entries is a few dozen stats - microseconds to a few milliseconds on a cold spindle - while the
// pre-fix behaviour was the whole remaining tree.
//
// The batch bounds STATS ONLY. Both walks hash whole files inside the same callback, so a batch of
// them would be 64 full-file hashes - minutes on a media tree, i.e. the window this guard exists to
// close. Each hashing branch therefore takes its own unconditional check immediately before reading
// the file, which is what actually bounds the abort to one in-flight hash.
const scanCtxCheckEvery = 64

// beforeHashHook is invoked by both walk+hash units immediately before they read a file's
// content, right after the unconditional context check that guards the read. It exists so a test
// can cancel at exactly the point the batched check cannot reach and prove the pre-hash guard is
// what bounds the abort; it is nil in production.
var beforeHashHook func()

// scanHint is the cheap-scan short-circuit fact for one tracked regular file: the baseline's
// recorded size, mtime (Unix seconds) and content hash. When a file's on-disk (size, mtime)
// still match the hint, scanView reuses hash instead of re-reading and re-hashing the content
// (Phase 9d), turning an idle pass from O(bytes in tree) into O(changed bytes).
type scanHint struct {
	size  int64
	mtime int64
	hash  string
}

// scanView walks the view's on-disk directory (syncRoot/<viewDir>, the view name or its
// numeric id) and returns a scanEntry for every regular file and directory it contains,
// in directory-walk (lexical, parents-before-children) order. It streams MD4 over each
// file rather than buffering it. Symlinks (and any path sitting behind a pre-existing
// symlink), non-containable paths, non-regular files, and the executor's staging temp files
// are skipped. (Keep-both conflict copies are deliberately NOT name-filtered - once Phase 6d
// pushes them they are ordinary server files; see the file header.) A view directory that does not exist yet yields an empty
// scan (a cold start before any inbound apply created it), not an error.
//
// It also returns the set of view-relative paths the walk observed on disk but did NOT
// surface as entries, each of which reconcile must treat as still-present (not vanished) so
// it never pushes a spurious OpRemove. A directory path in the set protects its whole subtree
// by prefix. EVERY skip branch of the walk records its path here - an unprotected skip is a
// silent remote deletion - which covers seven cases:
//   - a regular file the no-follow open found gone/swapped/non-regular (errUploadSourceGone),
//     i.e. a transient unlink/recreate race between readdir and open - defer to the next scan;
//   - a file that failed to open (EACCES) or to hash (an IO error) this pass;
//   - a sub-path the directory walk could not read (EACCES on a subtree) - skipped, not
//     fatal, so one unreadable subtree never blocks the whole view;
//   - an internal-artifact name (a staging temp) deliberately filtered out of the entries,
//     so a real user file sharing that name shape is not deleted;
//   - a symlink or other non-regular node (FIFO/socket/device) occupying a TRACKED path
//     (protectReplacedTracked) - the replacement is not a confirmed local
//     deletion, so the remote file is left untouched and a Warn surfaces the situation each
//     scan until the user resolves it; an UNTRACKED symlink/non-regular node stays simply
//     ignored (not in the set), preserving the long-standing behaviour for stray links;
//   - a path sitting behind a pre-existing SYMLINKED COMPONENT (noSymlinkEscape) - an
//     intermediate segment of a multi-segment view name, or a tracked subdirectory swapped for
//     a directory symlink mid-walk - which the walk can still enumerate but must not read;
//   - a non-containable path (containable), defense-in-depth: filepath.Rel cannot currently
//     produce one, but the protect discipline is total by construction.
//
// hints drives the Phase-9d cheap-scan short-circuit. When hints == nil scanView fully
// re-hashes every regular file (the original behaviour, used by cold start and the periodic
// safety-net pass). When hints != nil and a regular file's on-disk (size, mtime) still match
// its baseline hint, the stored hash is reused and the file is NOT opened or read - the idle
// cost drops from O(bytes in tree) to O(changed bytes). This is an optimization layered over
// the authoritative periodic full re-hash: a change the (size, mtime) pair fails to notice
// (a same-second modify that preserves size, within the mtime granularity) at worst adds
// latency until the next full pass, never loses data.
//
// ctx bounds the walk: the scan is the engine's single longest
// uninterruptible unit of work - O(bytes in the view) on a full re-hash or a cold start - and
// Run only checked ctx BETWEEN views, so a shutdown or a pause request landed behind hours of
// hashing on a multi-terabyte tree (under systemd, long enough to reach TimeoutStopSec and be
// SIGKILLed; under the Windows SCM, long enough to sit in STOP_PENDING - the window the shutdown
// checkpoints cover from the other side). Cancellation is reported as an ordinary scan error:
// the scan is read-only, and localReconcile returns before diffing on any scan error, so a
// half-finished walk can never be mistaken for a whole-view deletion.
func (e *Engine) scanView(ctx context.Context, viewID uint64, hints map[string]scanHint) ([]scanEntry, map[string]bool, error) {
	return e.scanViewFrom(ctx, viewID, "", hints)
}

// scanViewFrom is scanView over one subtree when under is set (a rescan, exclude.go): it walks
// from under - which it reports as an entry itself - and keys every path to the view root as
// scanView does. A missing subtree is an empty scan, as a missing view directory is.
func (e *Engine) scanViewFrom(ctx context.Context, viewID uint64, under string, hints map[string]scanHint) ([]scanEntry, map[string]bool, error) {
	name, viewAbs, err := e.resolveViewDir(viewID)
	if err != nil {
		return nil, nil, err
	}

	var entries []scanEntry
	unreadable := make(map[string]bool)
	// One-shot guard for the symlinked-component Warn below (WalkDir's callback is sequential,
	// so a plain bool is enough): every entry beneath one symlinked component would otherwise
	// log the same diagnostic.
	symlinkEscapeWarned := false
	// Per-pass memo of path prefixes already proven symlink-free, so the per-entry
	// noSymlinkEscape check below costs ~one Lstat per NEW component instead of re-walking
	// every entry's whole ancestor chain. WalkDir is parents-first and
	// its callback is sequential, so a plain map needs no lock and is always populated before
	// the children that reuse it. Dropped with this call, so a mid-pass symlink swap is caught
	// by the next scan (see symlinkMemo).
	symlinkOK := make(symlinkMemo)
	// Per-pass, lazily built index of the view's baseline FOLD keys, consulted by
	// protectReplacedTracked's case-variant fallback. Built at most once per scan and only if a
	// non-regular entry is actually seen; see trackedFoldIndex.
	trackedFolds := &trackedFoldIndex{viewID: viewID}
	// The directory whose entries the memo above currently describes. WalkDir proves a
	// directory in its own callback and only THEN calls ReadDir on it, so a proof taken
	// before that descent must not be reused for the entries the descent produced: dropping
	// the memo whenever the walk moves to another directory makes the first entry of every
	// directory re-Lstat the whole ancestor chain, including the directory just descended
	// into (noSymlinkEscapeMemo re-Lstats each entry's immediate parent on top of that).
	symlinkMemoDir := ""
	// scanned counts the entries the callback has seen, so the ctx check below costs one
	// atomic-ish read per scanCtxCheckEvery entries rather than one per entry: on a tree of
	// millions of small files the per-entry check would be measurable next to the stat the
	// walk already does, while a batch of scanCtxCheckEvery bounds the shutdown latency to
	// that many stats. Batching alone is NOT enough: the hashing branch below reads a whole
	// file inside this same callback, so a batch of scanCtxCheckEvery would be that many FULL
	// FILE HASHES on a full-rehash pass - minutes on a media tree, the very window this check
	// exists to close. The hash branch therefore checks unconditionally on its own, which is
	// what bounds the latency to the stats of one batch plus at most one in-flight hash.
	scanned := 0
	start := viewAbs
	if under != "" {
		start = filepath.Join(viewAbs, filepath.FromSlash(under))
	}
	walkErr := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		// Counted BEFORE the increment, so the predicate is "every Nth entry starting with the
		// first". The former scanned%N == 1 (counted after) degenerated to "never check" if
		// scanCtxCheckEvery were ever tuned to 1 - silently removing the guard.
		if scanned%scanCtxCheckEvery == 0 {
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
		}
		scanned++
		if err != nil {
			if p == start {
				// The view directory itself may not exist yet (no inbound apply has created it):
				// that is a clean empty scan, not a failure. Any other error on the root is
				// genuinely view-wide (e.g. EACCES on the root) and surfaces - localReconcile then
				// returns early before diffing, so it cannot mistake an unscannable view for a
				// whole-view deletion.
				if os.IsNotExist(err) {
					return filepath.SkipAll
				}
				return err
			}
			// A sub-path the walk could not read (an EACCES on a subdirectory or file, a transient
			// IO error). Aborting the whole walk here would fail the entire view scan every pass,
			// blocking ALL unrelated outbound sync for the view indefinitely. Skip just
			// this entry/subtree and continue, recording the path as unreadable so reconcile treats
			// it (and anything beneath it) as still-present rather than deleted - omission alone
			// would let a vanished baseline under here be read as a deletion and pushed as a
			// spurious remove. A directory's whole subtree is unscannable, so its path protects the
			// subtree by prefix in reconcile.
			rel, relErr := filepath.Rel(viewAbs, p)
			if relErr == nil {
				// Normalize to NFC so the protected-path set keys match the NFC scanEntry
				// paths and the NFC server/baseline keys downstream.
				unreadable[normpath.Normalize(filepath.ToSlash(rel))] = true
			}
			e.log.Warn("engine: scan skipping unreadable path", "path", p, "err", err)
			if d != nil && d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if p == viewAbs {
			return nil // the view root itself is not an entry
		}
		rel, err := filepath.Rel(viewAbs, p)
		if err != nil {
			return err
		}
		// Normalize every surfaced view-relative path to Unicode NFC so the store holds
		// one canonical encoding on all platforms (fixing the macOS NFD vs NFC mismatch
		// where the local scan and the server-authoritative baseline encode the same name
		// differently). The protected-path set keyed below shares the same NFC form.
		relSlash := normpath.Normalize(filepath.ToSlash(rel))
		base := d.Name()
		// Never surface the executor's own staging temps as user content. The match is restricted
		// to non-directory entries: os.CreateTemp only ever creates regular files, so a directory
		// whose name happens to fit the staging shape (e.g. a user dir ".cache.dl-123/") is NOT an
		// engine artifact and must be scanned normally - filtering it would wrongly exclude the
		// whole user subtree. The name match is also precise (isStagingName
		// matches only the engine's actual generated names, not any file ending in ".dl-"), so a
		// legitimate user file is not excluded from sync by a broad heuristic. Keep-both conflict
		// copies are intentionally NOT name-filtered: once the Phase-6d resolver pushes one it is
		// an ordinary server file, and a name match could only misfire on a real user file -
		// provenance comes from the DESIGN §13 `conflicts` list, not a name shape. Record the matched path as unreadable
		// so reconcile still treats it as still-present rather than vanished - engine artifacts
		// never carry a baseline, so this is purely belt-and-suspenders against an artifact briefly
		// aliasing a tracked path.
		if !d.IsDir() && isStagingName(base) {
			unreadable[relSlash] = true
			e.noteStagingName(viewID, p, d)
			return nil
		}
		// A ROOT name beginning with "id:" aliases the wire's id-addressing form (id:<file_id>
		// for download/resume_download; id:<parent_id>/<name> for upload), which shares the
		// single "path" parameter with view-relative addressing. Surfacing it would let a later
		// outbound Remove(viewID, "id:12345")/Rename round-trip the literal name into that
		// parameter; if the DSM dispatches by prefix it would mutate the FOREIGN node with
		// file_id 12345 - corrupting an unrelated node, recoverable only from the server recycle
		// bin. Until the server's parse semantics are confirmed with a live DSM
		// (DESIGN §13), do not surface such a path: record it in the unsurfaced set (pathProtected
		// treats it as still-present, so it is neither uploaded nor diffed as a create/delete/move
		// source) and Warn each scan. Only ROOT names are exposed (a nested "a/id:foo" carries the
		// prefix mid-parameter, not at the start); Windows cannot create a ':' in a filename, so
		// this is a no-op there. A directory's whole subtree is protected by prefix (SkipDir).
		if isRootIDAlias(relSlash) {
			unreadable[relSlash] = true
			e.log.Warn("engine: not syncing a root path that aliases id-addressing; leaving it untouched until resolved",
				"view", viewID, "path", relSlash)
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// An excluded path (exclude.go) is neither uploaded nor diffed, and - like every other
		// skip here - it is PROTECTED, so a baseline that somehow outlived the exclusion's drop
		// reads as still-present rather than as a local delete pushed to the server.
		if e.excluded(viewID, relSlash, nodeKindOf(d.IsDir())) {
			unreadable[relSlash] = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// Refuse redirecting entries - a symlink on every host, a junction too on Windows
		// (pathRedirects). WalkDir does not descend into either (neither
		// carries ModeDir), so reporting it and skipping is enough to keep the walk inside the
		// sync root. One occupying a TRACKED path is additionally protected:
		// silently omitting it would let local reconcile read the replacement as "the file
		// vanished" and push a remote delete for a node the user merely swapped locally.
		if pathRedirects(p, d.Type()) {
			kind := "symlink"
			if d.Type()&fs.ModeSymlink == 0 {
				kind = "junction"
			}
			e.protectReplacedTracked(viewID, relSlash, kind, unreadable, trackedFolds)
			return nil
		}
		// Reuse the inbound guards: a non-containable path or one sitting behind a
		// pre-existing symlink can never be a legitimate local change. Both branches record
		// the path in the unsurfaced set before skipping it, exactly like every other skip
		// branch of this walk: a skip that is NOT protected lets the merge-join read a still-
		// tracked baseline as "the file vanished" and push an OpRemove that deletes the live
		// remote node. Omission was survivable for neither guard -
		// noSymlinkEscape fires for EVERY entry of a view whose name has an intermediate
		// symlinked segment (WalkDir and the Lstat-based root check both traverse an
		// intermediate link, so the walk looks healthy while every entry is silently dropped:
		// the empty-scan debounce then confirms a whole-view delete), and for every child of a
		// tracked subdirectory swapped for a directory symlink between the parent's readdir and
		// the child's callback. containable CAN now reject a path filepath.Rel
		// produced - it also refuses a name this host will not spell
		// verbatim - so this arm is live rather than defense-in-depth, and the discipline must
		// be total: an unprotected skip is a silent remote deletion.
		if !containable(relSlash) {
			unreadable[relSlash] = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		// Scope the memo to the directory currently being listed: the walk has descended
		// into it since its own callback proved it, and a directory swapped for a symlink in
		// that window would otherwise let every child of the link target through on the
		// cached proof (see symlinkMemo). The reset is placed at the first entry of the
		// directory that reaches this check, so the full ancestor chain is re-Lstat'ed there
		// and reused only by its siblings.
		if parent := filepath.Dir(p); parent != symlinkMemoDir {
			symlinkMemoDir = parent
			clear(symlinkOK)
		}
		if symErr := e.noSymlinkEscapeMemo(name, rel, symlinkOK); symErr != nil {
			unreadable[relSlash] = true
			// One Warn per scan pass, not per entry: the symlinked component is shared by every
			// path beneath it, so a per-entry Warn would be a log wall over the whole view (and
			// the condition persists across passes until the user resolves it).
			if !symlinkEscapeWarned {
				symlinkEscapeWarned = true
				e.log.Warn("engine: scan skipping paths behind a symlinked path component; leaving them and the remote nodes untouched until resolved",
					"view", viewID, "path", relSlash, "err", symErr)
			}
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			// d.Info() lstats the entry; it can fail if the entry was removed or became
			// inaccessible between the parent readdir and now (ErrNotExist on a race, EACCES on a
			// permission change). Returning the error here would abort the WHOLE WalkDir and fail
			// the entire view scan every pass - wedging ALL unrelated outbound sync for the view
			// indefinitely, the same hazard the open/hash/sub-path branches already avoid. Treat it
			// like those: log, mark the path present-but-unread so reconcile does not mistake it for
			// a deletion and push a spurious remove (for a directory the key protects its subtree by
			// prefix), and skip just this entry/subtree rather than failing the scan.
			e.log.Warn("engine: scan skipping entry whose metadata could not be read", "path", p, "err", err)
			unreadable[relSlash] = true
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		mode := info.Mode()
		switch {
		case d.IsDir():
			id, hasID := dirIdentity(info)
			entries = append(entries, scanEntry{
				relPath:  relSlash,
				isDir:    true,
				mtime:    info.ModTime().Unix(),
				unixPerm: modeToDecimal(mode),
				dirID:    id,
				hasDirID: hasID,
			})
		case mode.IsRegular():
			// Cheap-scan short-circuit (Phase 9d): with hints supplied, if this file's on-disk
			// (size, mtime) still match its baseline hint, reuse the stored hash instead of
			// opening and re-hashing the content. Safety invariant: the cheap path reads no bytes
			// and produces no op that overwrites anything, so a missed change (same size+mtime,
			// different content within the mtime granularity) only adds latency to its OUTBOUND
			// push - it is caught by the periodic full re-hash (hints == nil) the loop forces every
			// limits.FullRehashInterval scans. A concurrent INBOUND modify for the same path does
			// not turn that latency into data loss: applyDownload re-hashes the on-disk file before
			// overwriting (see localDivergedFrom) and funnels an unrecorded local edit into the
			// keep-both resolver rather than clobbering it. The shared guards (containable, noSymlinkEscape)
			// above still ran before this branch; skipping the open only forgoes the per-open
			// no-follow recheck, which a swap would expose to the next full pass, never to a
			// content read here. size/mtime/perm come from the d.Info() lstat already done above.
			if hints != nil {
				if h, ok := hints[normpath.Key(relSlash)]; ok &&
					h.hash != "" && info.Size() == h.size && info.ModTime().Unix() == h.mtime {
					entries = append(entries, scanEntry{
						relPath:  relSlash,
						size:     info.Size(),
						mtime:    info.ModTime().Unix(),
						unixPerm: modeToDecimal(mode),
						md4:      h.hash,
					})
					return nil
				}
			}
			// Open and hash through the SAME no-follow, nonblocking descriptor the upload path
			// uses (openUploadSource), reading size/mtime/hash from that descriptor rather than
			// re-opening by name. d.Info() above is an lstat-time check; a name-based reopen
			// (os.Open) afterwards leaves a TOCTOU window: a swap to a FIFO would block the open
			// until a writer appears - wedging the whole engine loop - and a swap to a symlink
			// would make the open follow it and hash content outside the sync tree.
			// openUploadSource fails closed on both (O_NONBLOCK returns from a FIFO and the fstat
			// regular-file check rejects it; O_NOFOLLOW refuses every symlinked component) and
			// maps a vanished/swapped source to errUploadSourceGone - which here means "skip this
			// entry; the next scan reconciles the new on-disk reality".
			// About to read the whole file. The batched check above can be scanCtxCheckEvery
			// hashes behind, so check unconditionally here: this is what actually bounds a
			// shutdown or pause to ONE in-flight hash on a full-rehash pass instead of a
			// batch of them.
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			if beforeHashHook != nil {
				beforeHashHook()
			}
			f, fi, oerr := e.openUploadSource(p)
			if oerr != nil {
				if errors.Is(oerr, errUploadSourceGone) {
					// The walk saw a regular file here, but the no-follow open found it gone,
					// swapped, or no longer regular - a race between readdir and open. Record the
					// path as unreadable rather than silently omitting it: omission alone would let
					// local reconcile mistake the race for a deletion and push a spurious OpRemove.
					// Marked unreadable, the path is treated as still-present and left
					// for the next scan to reconcile against the settled on-disk reality.
					unreadable[relSlash] = true
					return nil
				}
				if pathVerifyUnsupported(oerr) {
					// Windows-only: the sync root sits on a filesystem that cannot resolve a handle
					// to a canonical path (GetFinalPathNameByHandle → ERROR_INVALID_PARAMETER /
					// ERROR_NOT_SUPPORTED - some redirector/FUSE mounts), so the no-follow open's
					// symlink-redirect security check cannot run and every source open fails
					// closed. Marking the path unreadable already keeps reconcile from
					// misreading the whole view as a mass deletion (uploads simply stall), but every
					// file would otherwise emit the generic "unreadable file" Warn below - a per-file
					// Warn-wall every scan. Collapse it to one clear once-per-view diagnostic; the
					// capability gap is a static property of the mount, re-probed on daemon restart.
					if !e.pathVerifyWarned[viewID] {
						e.pathVerifyWarned[viewID] = true
						e.log.Warn("engine: filesystem does not support path verification; view uploads disabled",
							"view", viewID, "path", p, "err", oerr)
					}
					unreadable[relSlash] = true
					return nil
				}
				// Any other open failure is local to this one entry - most notably EACCES on a
				// file the process cannot read (one owned by another user, or sitting behind a
				// component without the needed permission). Aborting the whole WalkDir here would
				// fail the entire view scan, and because the same file fails identically every
				// pass it would wedge ALL outbound sync (creates, modifies, deletes, moves) for
				// the view indefinitely. Treat it like a transient unreadable source instead: log,
				// mark the path present-but-unread (so reconcile does not mistake it for a
				// deletion and push a spurious remove), and continue the walk. A genuinely
				// view-wide failure (the walk root or a subdirectory unreadable) still surfaces
				// through the err!=nil callback path above.
				e.log.Warn("engine: scan skipping unreadable file", "path", p, "err", oerr)
				unreadable[relSlash] = true
				return nil
			}
			sum, herr := hashReaderMD4(f)
			_ = f.Close()
			if herr != nil {
				// A read error partway through hashing (an IO error, or the file truncated/locked
				// mid-read) is likewise local to this entry. Skip it as unreadable rather than
				// failing the whole view scan, for the same wedge-avoidance reason as the open
				// failure above.
				e.log.Warn("engine: scan skipping file that failed to hash", "path", p, "err", herr)
				unreadable[relSlash] = true
				return nil
			}
			entries = append(entries, scanEntry{
				relPath:  relSlash,
				size:     fi.Size(),
				mtime:    fi.ModTime().Unix(),
				unixPerm: modeToDecimal(fi.Mode()),
				md4:      sum,
			})
		default:
			// A non-regular, non-directory node (socket, device, named pipe): not syncable
			// content, skip it - protecting a TRACKED path like the symlink branch above, so
			// a FIFO/socket/device swapped onto a synced file is never read as a local delete.
			e.protectReplacedTracked(viewID, relSlash, mode.Type().String(), unreadable, trackedFolds)
		}
		return nil
	})
	if walkErr != nil {
		return nil, nil, fmt.Errorf("engine: scan view %d: %w", viewID, walkErr)
	}
	return entries, unreadable, nil
}

// protectReplacedTracked handles a symlink or other non-regular node (FIFO/socket/device)
// observed at relSlash during the walk. Such a node is never syncable content and is never
// surfaced as a scan entry - but when the path is TRACKED (a baseline exists), silently
// omitting it would let the local-reconcile diff read the replacement as "the file vanished"
// and push an OpRemove that deletes the still-present remote file. The
// policy is protect: the tracked path joins the unsurfaced set, so pathProtected treats it as
// still-present - never a vanished delete or a move source - and a Warn (once per scan pass
// per path) surfaces the situation; the remote file is left untouched until the user resolves
// the replacement locally. A directory baseline replaced by a symlink protects its whole
// subtree by prefix, exactly like an unreadable directory. An UNTRACKED node is simply
// ignored, as it always was - there is no baseline to mis-diff and warning about every stray
// symlink in the tree would be noise - so the store lookup runs only for the rare non-regular
// entries, never per regular file. A baseline lookup that fails for any reason other than
// not-found protects conservatively: a remote delete must never ride on an unverifiable
// "untracked" claim.
func (e *Engine) protectReplacedTracked(viewID uint64, relSlash, nodeType string, unreadable map[string]bool, folds *trackedFoldIndex) {
	_, err := e.store.GetBaseline(viewID, relSlash)
	if isNotFound(err) {
		// On a case-insensitive filesystem the replacement may occupy the tracked slot under
		// a different case than the stored baseline path (tracked "Doc.txt" removed, the
		// symlink created as "doc.txt" - the same logical slot): the exact-key lookup above
		// misses, the node would be read as an untracked stray, and the merge-join would
		// diff the base-only "Doc.txt" row as vanished - pushing an OpRemove that deletes
		// the still-present remote file. Retry by folded key (mirroring resolveParentID's
		// parentBaselineByFoldedKey) before concluding the node is untracked. normpath.Key
		// is NFC identity on a case-sensitive filesystem and both relSlash and the stored
		// baseline paths are NFC, so on Linux the fallback can only re-check what the exact
		// lookup already missed - it never conflates genuinely distinct paths. Recording the
		// SCAN spelling in the unsurfaced set below protects both spellings: pathProtected
		// folds its keys before comparing.
		tracked, ferr := folds.tracked(e, relSlash)
		switch {
		case ferr != nil:
			err = ferr // unverifiable: fall through to the conservative protect-with-Warn
		case !tracked:
			return // genuinely untracked under either spelling: ignore, as always
		default:
			err = nil // fold-matched a stored baseline: the slot is tracked, protect it
		}
	}
	unreadable[relSlash] = true
	if err != nil {
		e.log.Warn("engine: could not check whether a non-syncable node replaced a tracked path; protecting it",
			"view", viewID, "path", relSlash, "node_type", nodeType, "err", err)
		return
	}
	e.log.Warn("engine: tracked path replaced by a non-syncable node; leaving it and the remote file untouched until resolved",
		"view", viewID, "path", relSlash, "node_type", nodeType)
}

// trackedFoldIndex answers "does the view hold ANY baseline whose folded key (normpath.Key)
// matches this path's?" - the case-variant fallback protectReplacedTracked uses when its
// exact-key lookup misses. File and directory baselines both count: either means the logical slot
// is tracked and the replacement must be protected (a directory baseline protects its subtree by
// prefix downstream, since pathProtected folds).
//
// It is an INDEX rather than a per-question stream because the question is asked per non-regular
// entry and the answer is almost always "no". The stream stopped at the first match, so a MISS -
// the normal case, since a stray symlink is untracked - decoded and folded every baseline in the
// view, every scan pass. A tree carrying thousands of symlinks (a pnpm-style node_modules is built
// almost entirely of them) against a 100k-baseline view therefore cost thousands x 100k record
// decodes per pass, repeated every local_scan_interval under churn: an O(symlinks x baselines)
// CPU burn in what is meant to be the cheap steady-state pass.
//
// Built at most once per scan pass, and only if a non-regular entry is actually seen, so a tree
// with none pays nothing. The semantics are deliberately unchanged - same fold, same treatment of
// corrupt records, same conservative answer on a store failure - because this guard is what keeps
// a locally-swapped node from being diffed as a deletion and pushed out as an OpRemove against a
// live remote file.
type trackedFoldIndex struct {
	viewID uint64
	built  bool
	keys   map[string]struct{}
	err    error
}

// tracked reports whether relSlash's fold key matches any baseline in the view, building the
// index on first use. A build failure is remembered and re-reported: the caller protects
// conservatively on an error, and re-running a stream that just failed once per entry would
// only multiply the cost of a broken store.
func (ix *trackedFoldIndex) tracked(e *Engine, relSlash string) (bool, error) {
	if !ix.built {
		ix.built = true
		keys := make(map[string]struct{})
		skipped, err := e.scanBaselines(ix.viewID, func(pb store.PathBaseline) error {
			keys[normpath.Key(pb.Path)] = struct{}{}
			return nil
		})
		if err != nil && !errors.Is(err, errStopBaselineScan) {
			ix.err = err
		} else {
			// A record the stream skipped as corrupt still marks its slot
			// tracked: only the stored value is unreadable - the path, which is all the fold
			// match needs, comes from the intact key. Without this a corrupt record would read
			// as "untracked" and let a replacement ride an OpRemove out to the server.
			for _, p := range skipped {
				keys[normpath.Key(p)] = struct{}{}
			}
			ix.keys = keys
		}
	}
	if ix.err != nil {
		return false, ix.err
	}
	_, ok := ix.keys[normpath.Key(relSlash)]
	return ok, nil
}

// resolveViewDir resolves a view's on-disk directory: its validated name (the stored view
// name, or the numeric id when the name is empty) and the absolute path syncRoot/<name>. It
// mirrors abs's view-name validation so a name that escapes its subdirectory, cleans to "."
// (which would resolve the view to the sync root itself), carries a NUL, or is one the host
// will not spell verbatim is rejected.
//
// The representability gate has to hold HERE too, not only in abs: for a
// view named "Team." Win32 aliases syncRoot\Team. onto syncRoot\Team, so the scan would walk a
// DIFFERENT directory - a sibling view's, or unrelated user content - and surface every entry
// found there as an untracked local create of this view. Uploads of those creates dead-letter
// (applyUploadOut resolves through abs, which refuses the name), but applyMkdirOut validates
// only the view-RELATIVE path, so the aliased tree's directories would be pushed into this
// view server-side. Refusing the name suppresses the view's scan instead, and every caller
// already treats a resolve failure as "do not scan this view" (localScan, viewRootUnavailable).
func (e *Engine) resolveViewDir(viewID uint64) (name, viewAbs string, err error) {
	v, gerr := e.store.GetView(viewID)
	if gerr != nil {
		return "", "", fmt.Errorf("engine: scan resolve view %d: %w", viewID, gerr)
	}
	name = v.Name
	switch {
	case e.views.Flat:
		// A flat layout collapses the view onto the sync root: there is no
		// directory component, so none of the name checks below apply - the name is
		// never spelled on disk. Missing this branch was the whole-view delete: apply
		// wrote through abs() to the sync root while every local-side path here
		// still walked syncRoot/<name>, so the scan looked in a directory nothing
		// had been written to, found no file for any baseline, and detectRemoves
		// emitted an OpRemove for each - which the server-subtree check cannot
		// catch, because the server really does agree with the store. The files
		// were simply never looked for.
		return "", e.syncRoot, nil
	case name == "":
		name = strconv.FormatUint(viewID, 10)
	default:
		// The server path is slash-separated, so normalise to the OS separator and reject a
		// name that escapes its subdirectory or carries a NUL (filepath.IsLocal accepts "."
		// and an embedded NUL on Unix).
		name = filepath.FromSlash(name)
		if !filepath.IsLocal(name) || filepath.Clean(name) == "." || strings.IndexByte(name, 0) >= 0 {
			return "", "", fmt.Errorf("engine: scan view %d name %q does not map to a subdirectory", viewID, name)
		}
		if !hostRepresentablePath(name) {
			return "", "", fmt.Errorf("engine: scan view %d name %q cannot be represented verbatim on this host", viewID, name)
		}
	}
	return name, filepath.Join(e.syncRoot, name), nil
}

// viewRootUnavailable reports whether a view's on-disk root directory is in a
// known-unavailable state - it does not exist, is a symlink, or is not a directory - as
// opposed to present-and-readable (possibly empty). The local-reconcile empty-scan debounce
// (localreconcile.go) consults this so a vanished or symlinked root is never counted as
// confirmation of a whole-view delete: such a root is detectably unavailable and must not
// advance the debounce toward erasing every remote baseline. An unmounted mount point, by
// contrast, presents as an ordinary empty directory and CANNOT be told apart from a genuine
// empty-out from the filesystem alone, so that case is left to the debounce.
func (e *Engine) viewRootUnavailable(viewID uint64) (bool, error) {
	_, viewAbs, err := e.resolveViewDir(viewID)
	if err != nil {
		return false, err
	}
	fi, err := os.Lstat(viewAbs)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil // the root directory was never created or has been removed
		}
		return false, fmt.Errorf("engine: stat view root %q: %w", viewAbs, err)
	}
	// A symlinked root could redirect the whole view outside the sync tree; a non-directory
	// where a directory is expected is equally anomalous. Neither is a legitimate empty-out,
	// so neither should be read as the user having deleted the view's contents.
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return true, nil
	}
	return false, nil
}

// recordViewRootDevice captures the view root's current filesystem device id while the view is
// healthy (observed non-empty), so a later empty scan can tell a genuine empty-out from a mount
// that dropped or swapped underneath the root (the device id would then differ). It is
// best-effort: a resolve or stat failure simply leaves any prior recording in place rather than
// erasing the comparison baseline the empty-scan guard relies on.
//
// The id is also persisted to the view record (store.View.RootDevice) so the guard survives a
// daemon restart - the in-memory map is rebuilt empty on start, so without a durable baseline a
// restart into an already-dropped/swapped mount would bypass the guard. The
// persist is gated on an actual change so the steady-state non-empty scan (every few seconds)
// does no store write: only the first record after a restart, or a genuine device change, writes.
func (e *Engine) recordViewRootDevice(viewID uint64) {
	dev, ok := e.currentViewRootDevice(viewID)
	if !ok {
		return
	}
	e.recordViewRootDeviceID(viewID, dev)
}

// currentViewRootDevice resolves the view root's current filesystem device id, reporting false
// when the directory cannot be resolved or the id cannot be read. It is the single probe both
// recordViewRootDevice and viewRootDeviceChanged run, so a caller needing both answers (see
// adoptViewRootDevice) can stat the root once instead of twice per scan pass.
func (e *Engine) currentViewRootDevice(viewID uint64) (uint64, bool) {
	_, viewAbs, err := e.resolveViewDir(viewID)
	if err != nil {
		return 0, false
	}
	dev, err := e.deviceID(viewAbs)
	if err != nil {
		return 0, false
	}
	return dev, true
}

// recordViewRootDeviceID is recordViewRootDevice over an already-resolved device id.
func (e *Engine) recordViewRootDeviceID(viewID, dev uint64) {
	if prev, ok := e.viewRootDev[viewID]; ok && prev == dev {
		return // unchanged since the last scan; the persisted record already matches
	}
	e.viewRootDev[viewID] = dev
	e.persistViewRootDevice(viewID, dev)
}

// persistViewRootDevice records dev as the view's healthy root device id in the durable view
// record so the empty-scan mount-swap guard survives a daemon restart. Best-effort: a load or
// write failure is logged and leaves the in-memory recording (and any prior persisted value)
// intact rather than failing the scan over a bookkeeping write.
func (e *Engine) persistViewRootDevice(viewID, dev uint64) {
	v, err := e.store.GetView(viewID)
	if err != nil {
		e.log.Warn("engine: could not load view to persist root device id", "view", viewID, "err", err)
		return
	}
	if v.RootDevice == dev {
		return
	}
	v.RootDevice = dev
	if err := e.store.PutView(viewID, v); err != nil {
		e.log.Warn("engine: could not persist view root device id", "view", viewID, "err", err)
	}
}

// deviceGuardState is one view's cross-pass state for the view-root device guard. It holds no
// persisted facts: everything here is an observation of THIS process, so a daemon restart starts
// from the durable RootDevice baseline alone, exactly as before.
type deviceGuardState struct {
	// unreadable is set while the device id has been unreadable since the last successful read.
	// It exists so viewRootDeviceProbe's "treating root as unavailable" Warn is emitted once per
	// streak: a filesystem that stably refuses to report an id (FUSE, a network redirector, the
	// pathVerifyUnsupported class) hits this arm on every scan pass, forever.
	unreadable bool
	// proceedWarned is the same once-per-streak gate for adoptViewRootDevice's "proceeding
	// without a device baseline" Warn, which cannot self-silence by recording an id (there is
	// none to record) and would otherwise repeat every pass for a healthy such view.
	proceedWarned bool
	// confirmPath/confirmHash remember the file that last supplied content evidence
	// (scanConfirmsTrackedTree). The next pass re-checks that candidate FIRST, so a view whose
	// device stays changed/unreadable pass after pass pays one hash instead of re-running the
	// maxDeviceConfirmHashes-capped sweep. The evidence itself is still re-derived every pass -
	// the memo only reorders candidates, so a mount swapped under an unreadable id is still
	// caught by the ordinary sweep once the remembered file stops matching.
	confirmPath string
	confirmHash string
}

// rootDeviceProbe is one pass's reading of the view root's filesystem device id together with the
// id recorded while the view was last observed healthy. changed carries the guard's verdict: the
// root now sits on a different filesystem than when its content was last seen healthy - a mount
// that dropped or swapped underneath the path - or its device id can no longer be read at all (a
// Statfs/handle failure, readable == false). The local-reconcile empty-scan debounce consults it
// so such a root, which presents as an ordinary empty directory Lstat cannot distinguish from a
// genuine empty-out, is treated as unavailable and never counted toward erasing the remote view.
// changed is false when no healthy device id has been recorded yet (nothing to
// compare against) - the missing/symlink/non-directory checks and the debounce still guard that
// case.
type rootDeviceProbe struct {
	prev     uint64 // the recorded healthy device id, valid only when havePrev
	havePrev bool
	dev      uint64 // the id read this pass, valid only when readable
	readable bool
	changed  bool
}

// viewRootDeviceProbe reads the view root's current device id and compares it with the recorded
// healthy one. It is the single probe both the empty-scan guard and adoptViewRootDevice run, so a
// caller needing the comparison AND the id (to record it) stats the root once per scan pass.
func (e *Engine) viewRootDeviceProbe(viewID uint64) rootDeviceProbe {
	var p rootDeviceProbe
	expected, haveExpected := e.viewRootDev[viewID]
	if !haveExpected {
		// No in-memory baseline - typically a fresh daemon start, where the map is rebuilt
		// empty. Fall back to the device id persisted while the view was last healthy so the
		// guard survives a restart into an already-dropped/swapped mount. A
		// persisted 0 (never recorded, or a pre-upgrade view) means there is nothing to
		// compare against, so leave that case to the debounce as before.
		if v, err := e.store.GetView(viewID); err == nil && v.RootDevice != 0 {
			expected, haveExpected = v.RootDevice, true
			e.viewRootDev[viewID] = expected // seed the map so subsequent scans take the fast path
		}
	}
	p.prev, p.havePrev = expected, haveExpected
	_, viewAbs, err := e.resolveViewDir(viewID)
	if err != nil {
		return p // a name-resolution failure is reported by the caller's own checks
	}
	dev, err := e.deviceID(viewAbs)
	if err != nil {
		if !haveExpected {
			// Nothing recorded to compare against, so an unreadable root is not evidence of a
			// change; the caller's own missing/symlink/non-directory checks and the debounce
			// still guard it.
			return p
		}
		// The device id cannot be confirmed (the mount may be in the middle of dropping, or
		// the path is momentarily inaccessible): conservatively treat the root as unavailable
		// rather than risk reading a transient empty as a whole-view delete. Warn once per
		// streak - a filesystem that never reports an id would otherwise log this on every
		// scan pass forever.
		if st := e.deviceGuard[viewID]; !st.unreadable {
			st.unreadable = true
			e.deviceGuard[viewID] = st
			e.log.Warn("engine: view root device id unreadable while synced baselines exist; treating root as unavailable (logged once until the id reads again)",
				"view", viewID, "err", err)
		}
		p.changed = true
		return p
	}
	if st := e.deviceGuard[viewID]; st.unreadable {
		st.unreadable, st.proceedWarned = false, false
		e.deviceGuard[viewID] = st
		e.log.Info("engine: view root device id readable again", "view", viewID, "device", dev)
	}
	p.dev, p.readable = dev, true
	p.changed = haveExpected && dev != expected
	return p
}

// noteAdoptedDeviceMove records an old→new device transition this pass adopted on positive
// content evidence, so an EMPTY sibling view of the same filesystem can corroborate the same
// transition instead of suppressing its scans forever (deviceMoveCorroborated).
func (e *Engine) noteAdoptedDeviceMove(p rootDeviceProbe) {
	if !p.havePrev || !p.readable || p.prev == 0 || p.prev == p.dev {
		return
	}
	e.deviceMoves[p.prev] = p.dev
}

// deviceMoveCorroborated reports whether another view of this daemon has already adopted exactly
// the same old→new view-root device transition on positive content evidence during this process.
// That is the only evidence an EMPTY view can ever offer about its own device change: it has no
// content to compare, so without corroboration a routine renumbering (dm/LVM/btrfs/network mounts
// do not keep their device number across a reboot) would suppress its scans - and reset its
// debounce - forever, and a legitimate whole-view delete would never converge.
// Corroboration is deliberately narrow: the sibling must have started from the SAME
// recorded id (i.e. the same filesystem) and landed on the SAME new one, having walked its own
// still-tracked tree to prove it. A genuine mount swap produces no such adoption - the sibling's
// scan is suppressed for want of content evidence - so the empty view stays suppressed, which is
// the guard's fail-toward-protected direction.
func (e *Engine) deviceMoveCorroborated(p rootDeviceProbe) bool {
	if !p.havePrev || !p.readable || p.prev == 0 {
		return false
	}
	to, ok := e.deviceMoves[p.prev]
	return ok && to == p.dev
}

// adoptViewRootDevice is the NON-EMPTY scan's half of the mount-swap guard,
// the mirror of the empty-scan viewRootDeviceChanged check in localReconcileWith. It reports
// whether the scan may proceed to the diff, and records the current device id when it may.
//
// The non-empty branch used to record the device unconditionally and diff straight away. That
// made a mount swap under a POPULATED root the worst case rather than the guarded one: plugging
// the wrong USB disk, an fstab typo or a foreign dataset appearing at the view root presents as
// an ordinary non-empty directory, so the pass adopted the foreign volume's device id - erasing
// the very comparison baseline the empty-scan guard depends on - and then diffed with no
// debounce at all, emitting an OpRemove for every synced baseline the foreign tree lacks and an
// upload that overwrites the server copy of every path it happens to share. The machine held
// positive evidence that this is not the tracked filesystem and overwrote it with that evidence.
//
// The gate therefore requires positive CONTENT evidence before adopting a changed device
// (the review's open decision point, resolved toward confirmation rather than a bare suppression):
//   - no synced baseline yet → nothing a mis-diff could destroy; adopt.
//   - device unchanged (or none recorded / none persisted) → the ordinary steady state; adopt.
//   - device changed (or unreadable) AND the scanned tree still matches a tracked node
//     (scanConfirmsTrackedTree) → a legitimate remount or disk migration: the device number of
//     one filesystem is NOT stable across reboots (dm/btrfs/network mounts renumber), so this
//     case must adopt on the FIRST scan or every such reboot would freeze the view. Adopt, Warn.
//   - device changed and the tree matches nothing tracked → a foreign filesystem. Suppress the
//     whole scan (mirroring the empty branch's return) and KEEP the recorded device, so the
//     guard still holds on the next pass. Nothing is deleted, nothing is overwritten, and the
//     Warn repeats each scan until the operator restores the right mount (or unlinks the view).
//
// An UNREADABLE id is the one arm that cannot self-silence: there is no id to record, so a
// filesystem that never reports one (FUSE, a network redirector) re-enters the confirmed arm on
// every pass. Its Warn is therefore emitted once per unreadable streak, and the confirmation
// re-checks the remembered candidate first (scanConfirmsTrackedTree's memo) instead of re-running
// the capped sweep. The evidence itself is still re-derived every pass - caching the VERDICT
// would let a mount swapped under an unreadable id diff straight through, the very failure this
// gate exists to prevent.
func (e *Engine) adoptViewRootDevice(viewID uint64, rows []joinRow, hasSynced bool) bool {
	// One probe of the view root serves the comparison AND the recording below: the steady
	// state (synced baselines, unchanged device) runs on every scan pass of every view, so
	// re-resolving and re-statting the root a second time to record the id just compared is
	// pure duplication.
	p := e.viewRootDeviceProbe(viewID)
	if !hasSynced || !p.changed {
		if p.readable {
			e.recordViewRootDeviceID(viewID, p.dev)
		}
		return true
	}
	if !e.scanConfirmsTrackedTree(viewID, rows) {
		e.log.Warn("engine: view root device id changed or unreadable (mount swap) while synced baselines exist and the scanned tree matches none of them; suppressing this scan and keeping the recorded device",
			"view", viewID)
		return false
	}
	if !p.readable {
		if st := e.deviceGuard[viewID]; !st.proceedWarned {
			st.proceedWarned = true
			e.deviceGuard[viewID] = st
			e.log.Warn("engine: view root device id unreadable while the scanned tree still matches a tracked node; proceeding without a device baseline (logged once until the id reads again)",
				"view", viewID)
		}
		return true
	}
	e.log.Warn("engine: view root device id changed while the scanned tree still matches a tracked node; adopting the new device id (remount or disk migration)",
		"view", viewID, "previous_device", p.prev, "device", p.dev)
	e.noteAdoptedDeviceMove(p)
	e.recordViewRootDeviceID(viewID, p.dev)
	return true
}

// scanConfirmsTrackedTree reports whether a non-empty scan carries positive evidence that the
// tree it just walked is the tree the view's baselines describe, as opposed to a foreign
// filesystem that happens to sit at the view root. It is the confirmation adoptViewRootDevice
// requires before adopting a changed device id.
//
// Evidence is graded, strongest available wins:
//   - a synced FILE baseline joined to a scanned regular file with the SAME content hash. A
//     name match alone is weak (a foreign dataset can easily hold a "README.md" or a
//     "Documents/" at the same relative path); identical content is not - EXCEPT for empty
//     content, which every tree holds by the handful (.gitkeep, __init__.py, a lock file), so
//     a zero-byte candidate is skipped: its hash is a constant and matching it says nothing
//     about whose tree this is. A view tracking only empty files therefore supplies no file
//     evidence and stays suppressed, the guard's fail-toward-protected direction.
//   - only when the view tracks no hashable synced file at all (a directories-only view, or
//     one whose files were never hashed) does a synced DIRECTORY joined to a scanned directory
//     count - it is the strongest evidence such a view can offer.
//
// The file arm re-hashes the candidate from disk instead of trusting r.scan.md4. On the Phase-9d
// cheap scan (hints != nil, the default for every non-full-rehash pass) scanView COPIES the
// baseline's own hash into scanEntry.md4 whenever the on-disk (size, mtime) match the hint, so
// comparing the two would be tautological and the real predicate would degrade to "same path,
// same size, same mtime-second" - a stale mtime-preserving clone of the tree (rsync -a, a
// restored backup, the wrong snapshot) would then pass the mount-swap guard and get its content
// uploaded over the server copies, the very failure this guard exists to prevent. The re-hash is
// affordable because this runs only on the device-CHANGED branch, and it is capped at
// maxDeviceConfirmHashes candidates so a whole foreign tree of coincidental (size, mtime)
// matches cannot turn one scan pass into a full-tree hash.
//
// Rows are the post-collision/post-corrupt-drop join, so a protected node never supplies
// evidence. A view whose every tracked file changed while the device also changed produces no
// evidence and stays suppressed: that fails toward protected (a stalled view an operator sees
// in the log), never toward mass deletion - so an unreadable/vanished candidate simply yields no
// evidence rather than failing the pass.
func (e *Engine) scanConfirmsTrackedTree(viewID uint64, rows []joinRow) bool {
	// Re-check the candidate that supplied evidence last time FIRST. A view whose device stays
	// changed or unreadable pass after pass (the unreadable-id streak of a FUSE/network mount)
	// otherwise re-runs the whole capped sweep every scan; the memo makes the steady state one
	// hash. It is a reordering, not a cache of the verdict: the candidate must still be a synced
	// file row joined to a scanned file with the same recorded hash, and its bytes are re-read,
	// so the evidence is exactly as strong as the sweep's.
	if st, ok := e.deviceGuard[viewID]; ok && st.confirmPath != "" {
		if rowConfirmsMemo(rows, st.confirmPath, st.confirmHash) &&
			e.confirmTrackedFileContent(viewID, st.confirmPath, st.confirmHash) {
			return true
		}
		st.confirmPath, st.confirmHash = "", ""
		e.deviceGuard[viewID] = st
	}
	var hashableSynced, dirMatch bool
	var hashed int
	for _, r := range rows {
		b := r.base
		if b == nil || b.State != store.StateSynced {
			continue
		}
		isDir := b.FileType == "dir"
		if !isDir && b.LocalHash != "" {
			hashableSynced = true
		}
		if r.scan == nil {
			continue
		}
		switch {
		case isDir:
			if r.scan.isDir {
				dirMatch = true
			}
		case !r.scan.isDir && b.LocalHash != "" && b.LocalHash != emptyContentMD4 && b.LocalHash == r.scan.md4:
			// The cheap pre-filter agreed; confirm it against the actual bytes.
			if hashed >= maxDeviceConfirmHashes {
				continue
			}
			hashed++
			if e.confirmTrackedFileContent(viewID, r.scan.relPath, b.LocalHash) {
				// Remember the winner so the next pass of a still-changed/unreadable device
				// re-checks it directly instead of re-walking the capped candidate list.
				st := e.deviceGuard[viewID]
				st.confirmPath, st.confirmHash = r.scan.relPath, b.LocalHash
				e.deviceGuard[viewID] = st
				return true // content-identical tracked file: the tree is ours
			}
		}
	}
	return !hashableSynced && dirMatch
}

// rowConfirmsMemo reports whether rows still hold the joined synced-file row the memoized
// candidate was recorded from - the same path, the same recorded hash, joined to a scanned
// regular file. It replays the sweep's own gates for that one candidate so the fast path can
// never confirm on evidence the sweep would reject (a since-deleted baseline, a path that is now
// a directory, a rewritten hash).
func rowConfirmsMemo(rows []joinRow, path, hash string) bool {
	if hash == "" || hash == emptyContentMD4 {
		return false
	}
	for _, r := range rows {
		if r.base == nil || r.base.Path != path {
			continue
		}
		b := r.base
		return b.State == store.StateSynced && b.FileType != "dir" && b.LocalHash == hash &&
			r.scan != nil && !r.scan.isDir && r.scan.relPath == path
	}
	return false
}

// maxDeviceConfirmHashes bounds the real content hashes scanConfirmsTrackedTree performs in one
// pass. One match is all the evidence it needs, and the pre-filter (path + size + mtime + the
// recorded hash) already makes a non-matching candidate unlikely, so a small window keeps the
// device-changed branch cheap even when a foreign tree happens to mirror many names.
const maxDeviceConfirmHashes = 8

// confirmTrackedFileContent reports whether the view-relative relPath currently hashes to want.
// It resolves the canonical NFC scan path back to its real on-disk spelling (onDiskAbs) before
// hashing, so a byte-preserving filesystem holding an NFD name still supplies evidence, and
// reads through the same no-follow descriptor the upload path uses. Any resolve/open/hash
// failure simply reports "no evidence" - the caller's fail-toward-suppressed direction.
func (e *Engine) confirmTrackedFileContent(viewID uint64, relPath, want string) bool {
	abs, err := e.abs(viewID, relPath)
	if err != nil {
		return false
	}
	sum, err := e.hashLocalFileMD4(e.onDiskAbs(abs))
	if err != nil {
		return false
	}
	return sum == want
}

// hashReaderMD4 streams r through MD4 and returns the lowercase-hex digest, reading in
// chunks via io.Copy so a large source is never buffered whole. It hashes from an open
// descriptor so the outbound push can hash exactly the bytes it then streams (no second
// open that could race a swap); the caller rewinds the descriptor afterwards.
func hashReaderMD4(r io.Reader) (string, error) {
	h := md4.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// modeToDecimal converts an os.FileMode's permission bits (an octal value) into the
// wire-decimal form the protocol uses - the octal mode written as base-10 digits (e.g.
// 0o755 -> 755). It is the inverse of fsutil's decimalToMode, so a scanned perm round-
// trips back through fsutil.SetMeta. The conversion cannot fail for a valid permission
// (at most 0o777 -> "777" -> 777), so a parse error degrades to 0 (the wire's "unset").
func modeToDecimal(mode fs.FileMode) uint32 {
	d, err := strconv.ParseUint(strconv.FormatUint(uint64(mode.Perm()), 8), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(d)
}

// isStagingName reports whether base is one of applyDownload's staging temp files,
// created as os.CreateTemp(dir, "."+base+".dl-*"): a hidden file whose name ends in the
// ".dl-" marker followed by os.CreateTemp's numeric suffix. Such a file is mid-download
// and must never be scanned as user content.
//
// The match is precise (the ".dl-" marker MUST be followed by one or more digits and end
// the name) rather than a bare substring so a legitimate user file that merely contains
// ".dl-" in its name (e.g. ".notes.dl-draft") is not silently excluded from sync - the
// over-broad substring match dropped such files entirely, never uploading them or their
// later edits. The randomness os.CreateTemp appends is always decimal
// digits, so a real staging temp always satisfies the digit-suffix form.
func isStagingName(base string) bool {
	if !strings.HasPrefix(base, ".") {
		return false
	}
	i := strings.LastIndex(base, ".dl-")
	if i < 0 {
		return false
	}
	suffix := base[i+len(".dl-"):]
	if suffix == "" {
		return false
	}
	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
