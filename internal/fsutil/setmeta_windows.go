//go:build windows

package fsutil

import (
	"fmt"
	"os"
	"time"
)

// setMetaNoFollow on Windows applies only the mtime, and by name (os.Chtimes).
//
// Decision: the Unix no-follow descriptor discipline is
// deliberately NOT mirrored here. SetMeta never chmods on Windows (applyPerm is
// always false - Unix permission bits are meaningless there), so the primitive
// a symlink swapped onto the target in the promote→SetMeta window could
// redirect is reduced to stamping an mtime on the link's target - and creating
// a symlink on Windows requires an elevated or developer-mode token in the
// first place. A handle-based SetFileTime over a
// FILE_FLAG_OPEN_REPARSE_POINT CreateFile would close even that, but a bespoke
// CreateFile path is not worth it for a timestamp; accepted and recorded here.
func setMetaNoFollow(path string, mtime int64, _ os.FileMode, _ bool) error {
	if mtime == 0 {
		return nil
	}
	t := time.Unix(mtime, 0)
	if err := os.Chtimes(path, t, t); err != nil {
		return fmt.Errorf("fsutil: set mtime on %s: %w", path, err)
	}
	return nil
}
