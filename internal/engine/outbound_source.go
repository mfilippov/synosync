package engine

// This file carries the outbound upload-source / on-disk path resolution helpers split out
// of outbound.go (behaviour-preserving move, DESIGN §13). They open the local source for an
// upload while refusing to follow any symlink, and resolve a canonical (NFC) path to the real
// on-disk spelling on a byte-preserving filesystem.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/mfilippov/synosync/internal/normpath"
)

// openUploadSource opens the local file at abs for upload, refusing to traverse OR open any
// symlink along the path, then confirms via fstat that it is a regular file, returning the
// open descriptor and its FileInfo. The open walks the path one component at a time with
// openat(O_NOFOLLOW) (openUploadFileNoFollow): unlike os.Root - which blocks only a link
// whose target escapes the sync root but FOLLOWS an in-root relative symlink - this refuses
// EVERY symlink, including an intermediate component swapped for a link into a SIBLING view
// that still sits within the root. That closes the cross-view TOCTOU window abs/
// noSymlinkEscape (a pre-open check) leaves open: a directory along the path swapped for a
// symlink after the check can no longer redirect the read into another view, let alone
// outside the root. All upload facts - size, mtime, content, hash - are then read
// from this single descriptor rather than from name-based re-stats, so a swap after the open
// cannot be observed inconsistently.
//
// A vanished or replaced-by-symlink source maps to errUploadSourceGone so the op is dropped
// and the next scan reconciles, rather than wedging on a permanent error.
func (e *Engine) openUploadSource(abs string) (*os.File, os.FileInfo, error) {
	rel, rerr := filepath.Rel(e.syncRoot, abs)
	if rerr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, nil, fmt.Errorf("%w: upload source %q escapes the sync root", errRejectedPath, abs)
	}
	// openRel tracks the path actually opened: the literal NFC rel, or the resolved real
	// on-disk spelling if the NFC-tolerant fallback ran. The symlink classification below must
	// inspect this real path - not the canonical NFC abs, which need not exist on a
	// byte-preserving filesystem holding an NFD name.
	openRel := rel
	f, err := openUploadFileNoFollow(e.syncRoot, rel)
	if err != nil && os.IsNotExist(err) {
		// NFC-tolerant fallback. The scanner normalizes every observed name to Unicode NFC
		// before it reaches the store and the queued op (Phase 9b), but a byte-preserving
		// filesystem (Linux, and Windows for a decomposed name) keeps a file the user created
		// with NFD bytes under its original, non-NFC name - so the literal NFC path does not
		// exist on disk and a name-based open misses it. (macOS normalizes on the FS, so its
		// NFC open already resolves an NFD name and never reaches here.) Resolve the real
		// on-disk path by matching directory entries on their NFC form and retry once; the
		// retry still runs through the no-follow opener, which re-validates that no component
		// is a symlink, so this never widens the security envelope. A genuinely absent or
		// ambiguous source leaves the original not-exist error in place (errUploadSourceGone).
		if realRel, ok := e.resolveNFCPath(rel); ok && realRel != rel {
			openRel = realRel
			f, err = openUploadFileNoFollow(e.syncRoot, realRel)
		}
	}
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, errUploadSourceGone
		}
		// A source swapped for a symlink (for ANY path component) makes the no-follow open fail
		// closed - the security guarantee that no symlink is ever followed already holds at this
		// point. The exact errno differs (ELOOP for a final-component link, ENOTDIR for an
		// intermediate one), so classify the drop-vs-retry decision portably: if a component along
		// the path is now a symlink, the engine (which never creates symlinks, and whose scanner
		// skips them) reads it as the user having replaced the source - treat it as gone so the
		// next scan reconciles (producing the delete) rather than wedging on a permanent error.
		// Walk the path that was actually opened (the NFD spelling when the fallback ran), since
		// the canonical NFC abs may not exist on disk and would otherwise Lstat-miss the swap.
		if e.pathHasSymlinkComponent(filepath.Join(e.syncRoot, openRel)) {
			return nil, nil, errUploadSourceGone
		}
		return nil, nil, fmt.Errorf("open upload source %q: %w", abs, err)
	}
	fi, serr := f.Stat()
	if serr != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("stat upload source %q: %w", abs, serr)
	}
	if !fi.Mode().IsRegular() {
		_ = f.Close()
		// A non-regular node (a directory or FIFO substituted for the file) cannot be a valid
		// upload source. Reconcile it on the next scan rather than reading it.
		return nil, nil, errUploadSourceGone
	}
	return f, fi, nil
}

// pathHasSymlinkComponent reports whether any component of abs below the sync root currently
// redirects elsewhere (intermediate or final) - a symlink on every host, a junction too on
// Windows (pathRedirects) - walking with Lstat so it never follows one.
// It classifies an openUploadSource failure portably: the no-follow open already refused to
// follow the link (no disclosure), and this walk decides whether to drop the op as a
// user-replaced source (errUploadSourceGone, reconciled next scan) or surface a hard error.
// The sync root itself is not inspected - reaching it through a symlink is the operator's
// choice (matching noSymlinkEscape).
func (e *Engine) pathHasSymlinkComponent(abs string) bool {
	rel, err := filepath.Rel(e.syncRoot, abs)
	if err != nil {
		return false
	}
	cur := e.syncRoot
	for _, c := range splitComponents(filepath.FromSlash(rel)) {
		cur = filepath.Join(cur, c)
		fi, lerr := os.Lstat(cur)
		if lerr != nil {
			return false
		}
		if pathRedirects(cur, fi.Mode()) {
			return true
		}
	}
	return false
}

// resolveNFCPath maps an OS-separated, view-relative path whose components are in canonical
// Unicode NFC form (the form the scanner stored and queued) to the path that actually exists
// on disk, for the case where the filesystem preserves a non-NFC encoding byte-for-byte so the
// literal NFC path is absent (a user-created NFD name on Linux, or on Windows). It walks from
// the sync root; for each component it first probes the literal NFC name (the common case -
// an NFC-on-disk name, or a normalizing FS) and otherwise scans the parent directory for the
// single entry whose NFC form matches - a scan only a component that some other on-disk
// spelling could normalize onto needs at all (see asciiSelfEquivalent). It returns
// (resolved, true) only when every component
// resolved to exactly one on-disk name; an absent or ambiguous component yields ("", false) so
// the caller falls back to the literal path's not-exist handling. The result is always opened
// through the no-follow primitives, which re-validate that no component is a symlink, so the
// directory scan here cannot redirect a read outside the sync root.
func (e *Engine) resolveNFCPath(rel string) (string, bool) {
	return e.resolveNFCComponents(rel, false)
}

// resolveNFCPathPrefix is resolveNFCPath's materialising counterpart: it resolves rel's
// longest existing on-disk prefix the same way, but a component with no on-disk match under
// any spelling - a node that does not exist yet - keeps its literal canonical (NFC) name, as
// does everything below it. The apply paths that CREATE the node at rel (the download promote
// and mkdir) use it so an existing NFD-spelled target or parent directory
// is reused rather than shadowed by a parallel NFC twin entry, while a genuinely new leaf
// still materialises under NFC. An ambiguous component yields ("", false) exactly like
// resolveNFCPath; so does an unreadable directory, EXCEPT where the component under it is
// self-equivalent (asciiSelfEquivalent) and therefore never lists the directory at all - that
// component simply stays literal like any other unmatched one. Both callers guard on the
// result differing from rel, and a literal component is exactly what they would have used
// anyway, so the surviving path is never worse than the unresolved one.
func (e *Engine) resolveNFCPathPrefix(rel string) (string, bool) {
	return e.resolveNFCComponents(rel, true)
}

// resolveNFCComponents is the shared component walk behind resolveNFCPath (partial=false:
// every component must resolve) and resolveNFCPathPrefix (partial=true: the first unmatched
// component ends the walk and the remaining components stay literal). The directory listing
// is skipped for a component that is canonically equivalent only to itself
// (asciiSelfEquivalent), which is what keeps an inbound create off an O(entries) ReadDir of
// its parent.
func (e *Engine) resolveNFCComponents(rel string, partial bool) (string, bool) {
	comps := splitComponents(rel)
	if len(comps) == 0 {
		return "", false
	}
	curAbs := e.syncRoot
	out := make([]string, 0, len(comps))
	for i, c := range comps {
		// Fast path: the literal NFC name exists (an already-NFC on-disk name, or a
		// normalizing filesystem). Lstat does not follow a final symlink, and the eventual
		// no-follow open re-checks every component, so this is safe.
		if _, lerr := os.Lstat(filepath.Join(curAbs, c)); lerr == nil {
			out = append(out, c)
			curAbs = filepath.Join(curAbs, c)
			continue
		}
		match := ""
		// A self-equivalent component (the overwhelmingly common case: a plain ASCII name)
		// can only be the NFC form of its own byte string, so the Lstat miss above is already
		// the definitive answer and listing the parent cannot turn up a different on-disk
		// spelling. Skipping it keeps every inbound create - download promote, mkdir, rename,
		// keep-both - off an O(entries) ReadDir of its parent, which on a flat directory made
		// cold start quadratic.
		if !asciiSelfEquivalent(c) {
			entries, derr := e.listDir(curAbs)
			if derr != nil {
				return "", false
			}
			for _, ent := range entries {
				if normpath.Normalize(ent.Name()) != c {
					continue
				}
				if match != "" {
					// Two distinct on-disk names normalize to the same NFC form: ambiguous, so
					// refuse to guess - the caller reports the source as gone and the next scan
					// reconciles the real on-disk state.
					return "", false
				}
				match = ent.Name()
			}
		}
		if match == "" {
			if !partial {
				return "", false
			}
			// Nothing on disk matches this component under any spelling, so it - and
			// everything below it - does not exist yet: keep the remaining components
			// literal, the fresh-create tail that materialises under the canonical NFC name.
			return strings.Join(append(out, comps[i:]...), string(filepath.Separator)), true
		}
		out = append(out, match)
		curAbs = filepath.Join(curAbs, match)
	}
	return strings.Join(out, string(filepath.Separator)), true
}

// nfcAmbiguousASCII lists the only ASCII characters a NON-ASCII on-disk name can normalize
// onto. Exactly three code points have a pure-ASCII canonical (NFC) form - U+037E GREEK
// QUESTION MARK → ';', U+1FEF GREEK VARIA → '`' and U+212A KELVIN SIGN → 'K' - and NFC
// composition never produces ASCII from non-ASCII input, so a component free of these three
// characters cannot be the normalized spelling of anything but itself.
// TestNFCASCIISingletonsAreExhaustive re-derives the set over the entire code space, so an
// x/text update that introduced a fourth singleton would fail the build rather than silently
// skip a legitimate NFD fallback.
const nfcAmbiguousASCII = ";`K"

// asciiSelfEquivalent reports whether the path component c is canonically equivalent only to
// itself, which makes resolveNFCComponents' parent listing provably fruitless - the literal
// Lstat probe has already answered. True for pure-ASCII components containing none of
// nfcAmbiguousASCII; a name holding one of those three characters (or any non-ASCII byte)
// keeps the full listing fallback, so correctness is unchanged and only the wasted work goes.
func asciiSelfEquivalent(c string) bool {
	for i := 0; i < len(c); i++ {
		if c[i] >= utf8.RuneSelf || strings.IndexByte(nfcAmbiguousASCII, c[i]) >= 0 {
			return false
		}
	}
	return true
}

// localTargetGone reports whether nothing exists on disk at the view-relative path p - the
// Order B probe for an outbound Mkdir whose parent directory a preceding inbound OpRemove
// already deleted, carrying the local subtree (p included) into a _Conflict copy via
// preserveAside. It resolves p through onDiskAbs so an NFD-spelled directory on
// a byte-preserving filesystem is not misread as gone, then Lstat's the result: a not-exist
// miss means the target is gone (terminal drop), anything present (including an unresolvable
// path or a stat error other than not-exist) reports false so an ordinary errParentUnknown
// wait is preserved and the op stays queued. It never follows a symlink (Lstat).
func (e *Engine) localTargetGone(viewID uint64, p string) bool {
	abs, err := e.abs(viewID, p)
	if err != nil {
		return false
	}
	_, lerr := os.Lstat(e.onDiskAbs(abs))
	return os.IsNotExist(lerr)
}

// uploadSourceGone reports whether the local upload source at abs is gone - vanished,
// replaced by a symlink, or turned into a non-regular node - using the same no-follow
// classification as the real read path (openUploadSource → errUploadSourceGone). It is the
// Order B probe for an outbound Upload whose parent baseline resolveParentID can no longer
// resolve because a preceding inbound OpRemove of the whole parent directory already applied
// and preserveAside carried the edited subtree into a _Conflict copy. A present,
// still-openable regular file reports false so an ordinary errParentUnknown wait is preserved
// (the parent's own op may still run). A hard open error other than the gone sentinels also
// reports false, so a transient disk error keeps the op queued rather than dropping it.
func (e *Engine) uploadSourceGone(abs string) bool {
	f, _, oerr := e.openUploadSource(abs)
	if oerr != nil {
		return errors.Is(oerr, errUploadSourceGone)
	}
	_ = f.Close()
	return false
}

// onDiskAbs resolves a canonical (NFC) absolute path under the sync root to the path that
// actually exists on disk when a byte-preserving filesystem holds a non-NFC encoding
// (resolveNFCPath); when the literal path exists, or no normalize-equivalent entry is found,
// the input is returned unchanged so the caller observes the normal not-exist behaviour.
func (e *Engine) onDiskAbs(abs string) string {
	rel, err := filepath.Rel(e.syncRoot, abs)
	if err != nil {
		return abs
	}
	if realRel, ok := e.resolveNFCPath(rel); ok && realRel != rel {
		return filepath.Join(e.syncRoot, realRel)
	}
	return abs
}
