//go:build !linux && !darwin && !windows

package fsutil

// renameNoReplace falls back to the portable Lstat-guarded rename on platforms without a
// dedicated atomic no-replace primitive.
func renameNoReplace(from, to string) error {
	return renameNoReplaceGeneric(from, to)
}
