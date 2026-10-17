package engine

// This file carries the host name-representability gate.
//
// Every containment check the engine runs is LEXICAL: filepath.IsLocal, the "."-collapse
// rejection, the separator-aliasing rejection. Each of them answers "could this path escape
// the sync root?" - none answers "will the host materialise this name VERBATIM?". On Windows
// the answer is sometimes no: Win32 silently strips trailing dots and spaces from every
// component of a non-`\\?\` path, so os.Mkdir("foo.") creates "foo", os.Rename promotes onto
// "bar" when asked for "bar ", and Lstat("foo.") aliases "foo" - all with no error at all
// (empirically pinned on NTFS, Go 1.26). The engine reads "the call succeeded" as "the path I
// addressed is the path I landed", and every path-addressed gate downstream inherits the lie:
//
//   - an inbound create of "notes." writes bytes to "notes" and records a StateSynced baseline
//     for "notes." - a path that does not exist on disk. The next scan reads an untracked
//     create of "notes" plus a vanished "notes.", and move detection pushes the server rename
//     "notes." -> "notes": the server-side name is silently mutated for every replica.
//   - with the server holding BOTH "foo" and "foo.", the download of "foo." Lstats onto the
//     healthy, synced "foo": the never-lose funnel carries that innocent file off to a
//     _Conflict copy, the promote drops "foo."'s bytes into "foo"'s entry, and the following
//     scan pushes them over the server's "foo" and emits an OpRemove for the vanished "foo."
//     whose divergence precondition passes. Two server nodes destroyed, no user action.
//   - serialize.go's keys are the store spellings, so ops on "foo." and "foo" - one on-disk
//     file - parallelise across workers against the serialization invariant.
//
// The gate closes the class where the class begins: a path the host cannot spell verbatim is
// not containable, so reconcile skips the event BEFORE it mutates a baseline (the cursor still
// advances and the record simply stays on the server), and abs() refuses it as errRejectedPath
// so an op already in the queue dead-letters with an observable trace instead of mis-applying.
// The name stays intact server-side; the operator's remedy is to rename it on the server or on
// a peer that can represent it (DESIGN §6).
//
// This is deliberately NOT an errno classifier: there is no errno. nametoolong_windows.go used
// to claim Win32 rejects a trailing dot or space with ERROR_INVALID_NAME - it does not, and a
// dead-letter arm keyed on an error that never arrives is dead code.

// hostRepresentablePath reports whether every component of an OS-separated relative path
// names on disk exactly what it spells. It is true for every path on hosts that store names
// verbatim (all Unix-likes); see componentHostRepresentable for the per-OS rule.
func hostRepresentablePath(rel string) bool {
	for _, c := range splitComponents(rel) {
		if !componentHostRepresentable(c) {
			return false
		}
	}
	return true
}
