//go:build !windows

package fsutil

import "os"

// renameReplace is a plain os.Rename off Windows: a POSIX rename's permission to
// replace the destination comes from the parent directory's write bit, not from
// the destination file's own mode, so a mode-0444 destination is replaced without
// any special handling and there is nothing to retry.
func renameReplace(from, to string) error { return os.Rename(from, to) }
