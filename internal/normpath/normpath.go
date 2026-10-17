// Package normpath canonicalises sync paths so the engine holds one stable
// form regardless of the host filesystem's quirks.
//
// Two transforms are provided:
//
//   - Normalize folds a path to Unicode NFC on every platform, fixing the
//     macOS NFD ↔ NFC mismatch where the same user-visible name is encoded
//     differently by the local scan and the server-authoritative baseline.
//   - Fold additionally lower-cases the (already NFC) path on case-insensitive
//     filesystems so a case-only divergence is treated as the same node rather
//     than a spurious delete+create. On case-sensitive filesystems Fold is the
//     identity over the NFC form.
//
// The case-insensitivity flag defaults from the host GOOS (Windows/macOS
// case-insensitive, Linux case-sensitive) but is overridden at engine startup by
// a runtime probe of the actual sync-root filesystem (UseCaseInsensitive), so a
// case-insensitive mount under Linux - or a case-sensitive volume under macOS -
// folds the way the filesystem really behaves rather than the way the GOOS
// default assumes.
//
// Normalize is NOT injective, and not only in the NFD-versus-NFC direction every
// other comment in this tree describes. NFC also applies SINGLETON decompositions,
// mapping a compatibility duplicate onto a canonical twin that can be an entirely
// different character: U+212A KELVIN SIGN becomes ASCII "K", U+2126 OHM SIGN becomes
// U+03A9, U+212B ANGSTROM SIGN becomes U+00C5. So two distinct on-disk names can share
// one key with no combining marks and no case difference involved - on a case-SENSITIVE
// Linux filesystem, and with one of the two names looking like plain ASCII.
//
// The consequence is contained rather than special-cased, in two places that both key on
// Key and therefore cover this class for free: the engine's collision guards
// (scanKeyCollisions locally, catalogKeyCollisions over the server catalog) leave every
// colliding name protected and unsynced instead of letting a last-wins merge drop one;
// and outbound source resolution refuses to guess between two spellings, with
// nfcAmbiguousASCII naming the three code points whose NFC form is pure ASCII. Do not
// narrow either guard to NFD-shaped input - TestNormalizeCollapsesSingletons here and
// the engine's singleton collision tests exist to break such a change.
//
// The store keys stay the NFC path with original case preserved; folding is
// applied only to in-memory matching/serialization keys in the engine.
package normpath

import (
	"runtime"
	"strings"
	"sync/atomic"

	"golang.org/x/text/unicode/norm"
)

// caseInsensitive reports whether path matching should fold case. It defaults
// from the host GOOS (Windows and macOS are case-insensitive; Linux is
// case-sensitive) and is overridable in tests. It is an atomic.Bool because
// the test-only SetCaseInsensitive seam can flip it while live engine
// goroutines read it through Fold on their hot paths - a plain bool would be
// a data race under -race even though production never writes it.
var caseInsensitive = newCaseFlag(defaultCaseInsensitive(runtime.GOOS))

// newCaseFlag returns an atomic.Bool initialised to v.
func newCaseFlag(v bool) *atomic.Bool {
	var b atomic.Bool
	b.Store(v)
	return &b
}

// defaultCaseInsensitive returns the GOOS-based case-insensitivity default.
func defaultCaseInsensitive(goos string) bool {
	switch goos {
	case "windows", "darwin":
		return true
	default:
		return false
	}
}

// DefaultCaseInsensitive reports the host GOOS's case-insensitivity default
// (Windows and macOS case-insensitive, everything else case-sensitive). It is
// the value the flag holds until a runtime determination overrides it, and the
// fallback a failed sync-root probe reverts to.
func DefaultCaseInsensitive() bool {
	return defaultCaseInsensitive(runtime.GOOS)
}

// CaseInsensitive reports the process-wide flag currently in force - the GOOS
// default, or whatever a runtime determination (UseCaseInsensitive) or a test
// (SetCaseInsensitive) last set. It reads the same atomic Fold consults.
func CaseInsensitive() bool {
	return caseInsensitive.Load()
}

// UseCaseInsensitive sets the process-wide case-fold flag from a runtime
// determination - the engine's sync-root probe - overriding the GOOS default so
// a case-insensitive mount under Linux (exFAT/VFAT/NTFS, or an ext4 casefold
// directory), or a case-sensitive volume under macOS, matches paths the way the
// filesystem actually behaves rather than the way the host GOOS assumes. Unlike
// the test-only SetCaseInsensitive it returns no restore func: production sets
// the flag once at startup and never reverts it. The flag is atomic, so calling
// this once before the engine's goroutines start their hot-path Fold reads is
// race-free.
func UseCaseInsensitive(v bool) {
	caseInsensitive.Store(v)
}

// Normalize returns p folded to Unicode NFC. It is applied on all platforms so
// the store holds a single canonical encoding.
func Normalize(p string) string {
	return norm.NFC.String(p)
}

// Fold returns the case-folded matching form of p: the lower-cased NFC string
// on a case-insensitive platform, or the NFC string unchanged on a
// case-sensitive one.
//
// The fold is strings.ToLower (Unicode simple lowercase mapping) - a
// deliberate approximation of the host filesystem's own case rules, not a
// reimplementation of them. It diverges from the NTFS per-volume upcase table
// and from APFS's full Unicode case folding on edge characters; the known
// examples (pinned in TestFoldApproximationEdgeCases):
//
//   - U+0130 İ lowers to plain "i", colliding with I/i, while NTFS upcases
//     İ to itself (a false collision);
//   - U+1E9E ẞ lowers to U+00DF ß, staying distinct from "ss"/"SS", while
//     APFS full-folds both to "ss" (a false distinction);
//   - the Cherokee block's case pairs (U+13A0.. ↔ U+AB70.., Unicode 8.0)
//     collide here but are distinct under NTFS upcase tables frozen at
//     volume-format time before those mappings existed.
//
// Both failure directions are bounded: a false collision - two distinct
// on-disk names folding to one key - is caught by the scanKeyCollisions
// guard, which Warns and leaves both files protected/unsynced;
// a false distinction - names the filesystem considers case-equal read as two
// nodes - degrades to the pre-Fold spurious delete+create for those rare
// characters. Neither loses data silently, so OS-faithful case tables are
// deliberately out of scope (DESIGN §13).
func Fold(p string) string {
	n := Normalize(p)
	if caseInsensitive.Load() {
		return strings.ToLower(n)
	}
	return n
}

// Key is the convenience matching key Fold(Normalize(p)). Use it whenever two
// paths must be compared for "same node" semantics.
func Key(p string) string {
	return Fold(p)
}

// SetCaseInsensitive overrides the package-level case-insensitivity flag and
// returns a function that restores the previous value. The default is derived
// from the host GOOS; this hook lets a test exercise both the case-sensitive and
// case-insensitive matching branches regardless of the platform it runs on. The
// flag is atomic, so flipping it while engine goroutines are still calling Fold
// is race-free (each Fold sees either the old or the new value, never a torn
// read).
func SetCaseInsensitive(v bool) func() {
	prev := caseInsensitive.Swap(v)
	return func() { caseInsensitive.Store(prev) }
}
