package fsutil

// RenameReplace moves the entry at from onto to, replacing an existing
// destination - the semantics of os.Rename (POSIX rename, MOVEFILE_REPLACE_EXISTING
// on Windows). Unlike Rename it neither creates the destination's parent chain nor
// fsyncs the affected directories: it is the bare primitive, exported for the
// engine's staging-temp promote, which owns that sequencing itself
// (engine/inbound_download.go).
//
// On Windows the move is a MoveFileEx with MOVEFILE_REPLACE_EXISTING and
// MOVEFILE_WRITE_THROUGH, so the promote is flushed to disk before it returns -
// the same durability renameNoReplace already asks for, and the other half of the
// Windows directory-flush discipline (see syncdir_windows.go).
//
// It also retries the move ONCE after clearing a FILE_ATTRIBUTE_READONLY
// destination. MoveFileEx(MOVEFILE_REPLACE_EXISTING) fails with
// ERROR_ACCESS_DENIED when the destination carries that attribute (verified on
// the host), and unlike genuine ACL noise the attribute never clears itself: the
// apply is deterministically impossible, yet the executor classifies a lone
// ErrPermission as transient (a read-only destination is indistinguishable from
// ACL noise by errno alone), so a replacing download promote re-downloaded the
// whole body into a staging temp and failed at the rename on every drain, forever
// - O(size) of NAS traffic per drain cycle for as long as the daemon runs
// Clearing the attribute is safe at this point precisely
// because the caller has already proved the destination may be replaced: the
// engine's replacing promote runs behind the divergence gate (the on-disk content
// equals the last-agreed base) and the inbound rename behind guardRenameDest, so
// the read-only bit is metadata of content this client is entitled to overwrite.
// The promoted file carries the staging temp's attributes, so a destination that
// took the retry loses its read-only marker: SetMeta applies the wire permission
// on Unix only (atomic.go's applyPerm gate), so nothing restores the bit on
// Windows. That is a deliberate, data-safe residual (DESIGN §13) - the divergence
// gate already proved the CONTENT replaceable, and re-marking the promoted file
// read-only would make the next inbound modify pay the retry again - but it is a
// user-visible attribute loss on every such inbound modify, not a no-op.
//
// The retry is deliberately narrow: it fires only for a REGULAR destination that
// actually carries the attribute, so a directory occupant (ERROR_ACCESS_DENIED
// too, and the shape engine/executor.go dead-letters via permissionBlockedByDirTarget)
// and genuine ACL denial keep their original error verbatim and their existing
// classification. When the second move fails anyway the attribute is restored
// best-effort and the FIRST error is returned, so the failure the executor sees is
// the one the destination actually produced.
//
// On every other platform it is os.Rename unchanged: a POSIX rename's permission
// to replace comes from the parent directory, not the destination's own mode, so
// there is no equivalent shape.
func RenameReplace(from, to string) error {
	return renameReplace(from, to)
}
