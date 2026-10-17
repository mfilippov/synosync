package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/normpath"
)

// This file carries the 8.3 short-name alias gate, the second half of the
// "a path-addressed call answers for a DIFFERENT node than the one it spells" class whose first
// half is namemangle.go's trailing dot/space strip.
//
// NTFS (and FAT) keep an auto-generated 8.3 "short name" alongside every long name whose
// spelling does not already fit 8.3, and - when short-name generation is enabled on the volume,
// which it is by default on the system volume - that alias is a fully addressable second name
// for the SAME directory entry. Empirically pinned on this host:
//
//	os.WriteFile("Document Name Long.txt", …)          // creates one entry
//	os.ReadFile("DOCUME~1.TXT")                        // returns ITS bytes
//	os.Lstat("DOCUME~1.TXT").Name() == "DOCUME~1.TXT"  // Lstat echoes what it was asked
//	os.MkdirAll("SUBDIR~1")                            // no-ops onto "Subdirectory Long Name"
//
// So a perfectly legal server-side name that happens to be spelled like an 8.3 alias -
// "DOCUME~1.TXT" is a name a Linux or macOS peer may create at will - resolves, on a Windows
// client, onto a completely unrelated, already-tracked local file. Every path-addressed gate
// downstream then reasons about the wrong node, and nothing in the engine can notice: IsLocal
// accepts the name, normpath.Fold does not model the aliasing (so neither serialize.go's overlap
// keys nor scanKeyCollisions fire), and the store keys stay distinct.
//
// The damage is the mirror image of namemangle.go's. A fresh inbound create of "DOCUME~1.TXT"
// finds untrackedFilePresent "occupied" - it hashed the NEIGHBOUR's content - and the never-lose
// funnel carries that healthy, synced long-named file off to a _Conflict copy (the aside rename
// addresses the short name, so it physically moves the neighbour). The promote then materialises
// the literal "DOCUME~1.TXT", the next scan reads "Document Name Long.txt" as vanished, and the
// outbound pass emits an OpRemove whose divergence precondition passes: the server's long-named
// file is deleted for every client. The mkdir mirror grafts a whole inbound subtree inside the
// neighbouring directory; the rename mirror has guardRenameDest preserve a stranger's file.
//
// The gate closes the class the same way the namemangle.go gate does - by refusing to address what
// the host will not spell verbatim. A materialising apply resolves its path through
// onDiskAbsPrefixNoSymlink, which asks the host for the REAL spelling of the path's existing prefix
// and refuses with errRejectedPath when a component the op addressed is an alias for a
// differently-named entity. The op dead-letters with an observable trace, the neighbour is
// untouched, and the server-side record survives for the operator to rename on a peer that can
// address it (DESIGN §13).
//
// Scope is deliberately the MATERIALISING verbs (download promote, mkdir, rename destination,
// keep-both's canonical promote and copy destination) - the prefix resolver, not the all-or-
// nothing one. That is where an alias converts into a destructive apply against an innocent
// node. The destructive verbs addressing an EXISTING record (remove, rename source) are reached
// only through a baseline this client recorded, and recording one requires the create the gate
// now refuses, so the class is closed transitively for anything this client syncs; a record
// predating the fix is the documented residual (DESIGN §13). A full aliasing model in the fold
// keys is deliberately not built.

// rejectShortNameAlias reports errRejectedPath when any existing component of abs - an absolute
// path under the sync root - is an 8.3 short-name alias for an entity the host actually spells
// differently. It compares only the components BELOW the sync root: the operator's sync root may
// itself sit under a short-named ancestor (a %TEMP% under "C:\Users\MFILIP~1" is the ordinary
// case), which is not the engine's business and must not reject every apply.
//
// A host that cannot answer the question - the resolve failed for anything other than "no such
// path" - is inconclusive and the apply proceeds: short names are an NTFS/FAT feature exposed
// through the very call that failed, so a filesystem that refuses it is not one that aliases.
// Failing closed here would dead-letter every materialising op on such a mount, which is a worse
// outcome than the pre-gate status quo.
func (e *Engine) rejectShortNameAlias(abs string) error {
	if e.longPath == nil && !hostAliasesShortNames {
		return nil
	}
	rel, err := filepath.Rel(e.syncRoot, abs)
	if err != nil {
		return nil
	}
	// abs is the sync root itself, or outside it: the root's own spelling is the operator's
	// choice (a %TEMP% under "C:\Users\MFILIP~1" is ordinary) and escapes are abs()'s business.
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil
	}
	depth := len(splitComponents(rel))
	if depth == 0 {
		return nil
	}
	// Walk up to the longest existing prefix: the trailing components of a materialising apply
	// do not exist yet by construction, and the host reports "no such path" for them.
	cur := abs
	for ; depth > 0; depth-- {
		real, lerr := e.longPathName(cur)
		if lerr != nil {
			if os.IsNotExist(lerr) {
				cur = filepath.Dir(cur)
				continue
			}
			return nil // inconclusive; see the doc comment
		}
		spelled, actual, aliased := aliasedComponent(cur, real, depth)
		if !aliased {
			return nil
		}
		return fmt.Errorf("%w: path component %q is a short-name alias for %q; refusing to apply onto another node",
			errRejectedPath, spelled, actual)
	}
	return nil
}

// longPathName resolves a path to the spelling the host actually stores, routed through the
// e.longPath seam when one is wired and the per-OS resolver otherwise, so an Engine built as a
// literal in a test behaves exactly like a production one.
func (e *Engine) longPathName(name string) (string, error) {
	if e.longPath != nil {
		return e.longPath(name)
	}
	return osLongPathName(name)
}

// aliasedComponent compares the trailing n components of the addressed path against the same
// positions of the real spelling the host resolved it to, and returns the first pair that
// differs. Comparison is NFC-folded and case-insensitive: a case-only difference is how a
// case-insensitive volume ordinarily answers (the resolver echoes the case it was handed), and
// an NFD-stored name reached through the NFC-tolerant resolution is the same entry, not an
// alias. A short name never survives either folding - "DOCUME~1.TXT" and "Document Name
// Long.txt" differ in more than case.
//
// A real spelling with fewer than n components cannot be compared positionally, so it is
// reported as no alias rather than guessed at.
func aliasedComponent(addressed, real string, n int) (spelled, actual string, ok bool) {
	a := trailingComponents(addressed, n)
	r := trailingComponents(real, n)
	if a == nil || r == nil {
		return "", "", false
	}
	for i := range a {
		if !strings.EqualFold(normpath.Normalize(a[i]), normpath.Normalize(r[i])) {
			return a[i], r[i], true
		}
	}
	return "", "", false
}

// trailingComponents returns the last n components of p, or nil when p has fewer than n
// components above its volume root.
func trailingComponents(p string, n int) []string {
	out := make([]string, n)
	for i := n - 1; i >= 0; i-- {
		parent := filepath.Dir(p)
		if parent == p {
			return nil
		}
		out[i] = filepath.Base(p)
		p = parent
	}
	return out
}
