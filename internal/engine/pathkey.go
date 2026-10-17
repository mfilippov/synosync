package engine

// Containment over normpath fold keys.
//
// "Is this path beneath that one" is asked all over the engine - a type-replaced subtree,
// a collision that shadows its descendants, a queued move that re-bases what sits under
// it, a delete that supersedes the paths inside it, a directory removal enumerating its
// contents - and every site had spelled it as its own strings.HasPrefix with its own
// "/" concatenated on. Seven copies of one predicate is seven chances for one of them to
// forget that "a" must not match "ab", or to drop the fold, so a case-only or NFD-only
// difference in a name silently stops matching on the platforms where the filesystem
// treats them as the same node.
//
// Both take keys that are ALREADY folded (normpath.Key), because the callers hold folded
// keys and folding again inside would hide a caller that forgot to.

import (
	"strings"

	"github.com/mfilippov/synosync/internal/normpath"
)

// keySep is the separator a fold key is joined with. It is "/" and not the OS separator:
// a fold key is a canonical, platform-independent spelling, so a Windows client and a
// Linux client key the same server path identically.
//
// This is exactly where serialize.go's isAncestor differs, and the two must NOT be
// merged: it compares anchored, cleaned, OS-separator paths (what cleanKey produces) and
// gives the synthetic root a special case. Same question, different alphabet.
const keySep = "/"

// keyUnder reports whether child lies STRICTLY beneath dir. A path is not under itself,
// and a name that merely starts with dir's characters ("photos-old" against "photos") is
// not under it either - the separator is what makes it containment rather than a string
// prefix.
func keyUnder(child, dir string) bool {
	return strings.HasPrefix(child, dir+keySep)
}

// keyAtOrUnder reports whether child IS dir or lies beneath it. Used where a rule applies
// to a subtree including its own root - a directory removal, a containment gate - as
// opposed to the descendants-only cases keyUnder covers.
func keyAtOrUnder(child, dir string) bool {
	return child == dir || keyUnder(child, dir)
}

// rebaseMoved re-roots child (which lies under oldRoot, matched case-foldedly) onto
// newRoot. It strips oldRoot's leading segments by count rather than by byte length,
// because a case fold can change byte length (İ U+0130 → "i̇") and make the folded
// prefix match even when the original byte lengths differ - slicing child by
// len(oldRoot) would then cut mid-segment (or mid-rune). The caller guarantees child
// has at least oldRoot's segment count followed by a "/"-separated suffix.
func rebaseMoved(child, oldRoot, newRoot string) string {
	rest := child
	for n := strings.Count(oldRoot, "/") + 1; n > 0; n-- {
		i := strings.IndexByte(rest, '/')
		if i < 0 {
			// Shouldn't happen: the folded prefix test guarantees the slashes. Fall back to
			// the bare new root rather than producing a corrupted path.
			return newRoot
		}
		rest = rest[i+1:]
	}
	return newRoot + "/" + rest
}

// relocateUnderMove reports where cur ends up when a node moves from `from` to `to`, and
// whether that move touched it at all: cur may BE the moved node, or sit anywhere beneath
// it, and a directory move carries its whole subtree.
//
// Three walks over event and queue history each spelled this out for themselves - the
// forward supersession walk, the backward pre-batch-path walk (which passes the move
// reversed, since it is reading history backwards), and the outbound resolution of a path
// through pending renames. Each had its own two-arm switch, and each had to independently
// remember to fold before comparing and to re-base by segment count rather than by byte
// length. Direction is the caller's business; the relocation rule is not.
func relocateUnderMove(cur, from, to string) (string, bool) {
	ck, fk := normpath.Key(cur), normpath.Key(from)
	switch {
	case ck == fk:
		return to, true
	case keyUnder(ck, fk):
		return rebaseMoved(cur, from, to), true
	}
	return cur, false
}
