package engine

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/mfilippov/synosync/internal/normpath"
)

// casefoldProbePattern is the os.CreateTemp pattern for the sync-root case-fold
// probe file. It is a staging-style hidden name - a leading "." and a trailing
// ".dl-<digits>" marker - so both the scanner's isStagingName filter and the
// watcher's shouldIgnore filter suppress it: a probe file left behind by a crash
// between create and remove is never scanned as user content nor woken on. The
// name also carries ASCII letters (in "synosync-casefold-probe" and "dl") so its
// case can be flipped for the Lstat probe below.
const casefoldProbePattern = ".synosync-casefold-probe-*.dl-0"

// applyCaseFold probes the sync root's actual case-fold behaviour and sets the
// process-wide normpath flag from it. Left on the GOOS default, the
// engine would fold case wrongly on a case-insensitive mount under Linux
// (exFAT/VFAT/NTFS, or an ext4 casefold directory) - treating the server's
// legitimate "A.txt"/"a.txt" pair as one node and spuriously
// renaming/deleting/keep-both-copying one of them - or, symmetrically, on a
// case-sensitive volume under macOS. A probe failure (root missing/unwritable,
// or an Lstat error) falls back to the GOOS default, so behaviour is never worse
// than before the probe existed. It runs once at Run startup, before any
// Fold-consuming goroutine (the watcher/refresh/loop), so the atomic store is
// observed by every later read.
func (e *Engine) applyCaseFold() {
	if e.caseFoldProbe == nil || e.syncRoot == "" {
		// No probe seam, or no root to probe against (an unconfigured engine, e.g. a
		// bare unit test): keep the GOOS default the normpath package already holds.
		return
	}
	def := normpath.DefaultCaseInsensitive()
	ci, err := e.caseFoldProbe(e.syncRoot)
	if err != nil {
		e.log.Warn("engine: sync-root case-fold probe failed; using platform default",
			"root", e.syncRoot, "case_insensitive", def, "err", err)
		normpath.UseCaseInsensitive(def)
		return
	}
	normpath.UseCaseInsensitive(ci)
	if ci != def {
		e.log.Info("engine: sync-root filesystem case-fold differs from platform default",
			"root", e.syncRoot, "case_insensitive", ci, "platform_default", def)
	}
}

// probeSyncRootCaseFold determines empirically whether root's filesystem folds
// case. It creates a uniquely-named hidden probe file (removed before returning)
// and then Lstats the same name with its ASCII letters case-flipped: if the
// flipped name resolves to the very same file the filesystem is case-insensitive;
// if it does not exist the filesystem distinguishes case. Any I/O error (root
// missing or unwritable) is returned so the caller falls back to the GOOS
// default rather than guessing.
//
// Residual (deferred by decision - see DESIGN §13): the probe
// samples only the sync ROOT's filesystem, and normpath carries a single
// process-wide case-fold flag. A view directory that is a separate mount point with
// a DIFFERENT fold behaviour (an exFAT mount under an ext4 root, or the reverse) is
// therefore matched with the root's flag, which may be wrong for that view - the
// same wrong-fold class the root probe closes for the root, still open for a
// per-view mount whose fold=false direction the NFD/NFC fold-key collision guard does not
// cover. A
// per-view probe is intentionally NOT added: one global normpath flag cannot
// represent a mixed set of folds anyway, so the operational contract is simply not to
// mix case-fold behaviours under one sync root; a genuine per-view solution needs
// per-view fold state in normpath, a larger change left for future work.
func probeSyncRootCaseFold(root string) (bool, error) {
	if root == "" {
		// os.CreateTemp("") would silently fall back to the system temp dir and probe
		// THAT filesystem instead of the (unconfigured) sync root, so reject it up front.
		return false, errors.New("engine: empty sync root for case-fold probe")
	}
	f, err := os.CreateTemp(root, casefoldProbePattern)
	if err != nil {
		return false, err
	}
	name := f.Name()
	// The handle is not needed - only the directory entry - so close it now and remove
	// the entry on the way out regardless of outcome.
	_ = f.Close()
	defer func() { _ = os.Remove(name) }()

	fi, err := os.Lstat(name)
	if err != nil {
		return false, err
	}
	base := filepath.Base(name)
	flipped := flipASCIICase(base)
	if flipped == base {
		// No ASCII letters to flip (cannot happen with casefoldProbePattern, but guard
		// rather than mis-probe a nameless case). Treat as case-sensitive.
		return false, nil
	}
	ffi, err := os.Lstat(filepath.Join(filepath.Dir(name), flipped))
	if err != nil {
		// The case-flipped name does not resolve → the filesystem distinguishes case.
		// A non-existent flipped name is the probe's answer, not a failure.
		return false, nil
	}
	// A coincidentally-existing differently-cased file would also resolve; confirm it is
	// the same inode before concluding case-insensitivity.
	return os.SameFile(fi, ffi), nil
}

// flipASCIICase toggles the case of every ASCII letter in s, leaving all other
// bytes - digits, punctuation, and multi-byte (non-ASCII) rune bytes - untouched.
// It is used only to derive the case-flipped probe name; non-ASCII names still
// carry the ASCII "synosync-casefold-probe" / "dl" letters, so a flip is always
// produced.
func flipASCIICase(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		switch c := b[i]; {
		case c >= 'a' && c <= 'z':
			b[i] = c - ('a' - 'A')
		case c >= 'A' && c <= 'Z':
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
