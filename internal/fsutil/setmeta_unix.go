//go:build !windows

package fsutil

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// setMetaNoFollow applies the already-decoded metadata through a single
// no-follow descriptor so the target path is resolved exactly once:
// the engine's whole symlink discipline runs BEFORE/AT the
// promote-rename, but SetMeta runs after it, and the former name-based
// os.Chmod/os.Chtimes re-resolved the path - following a symlink swapped onto
// the freshly promoted file in that window, so server-controlled mode bits
// (including the setuid/setgid mapping) landed on the link's target
// anywhere outside the sync tree. O_NOFOLLOW fails the open on a symlink
// (ELOOP), and the fd-based futimes/fchmod then bind both mutations to the
// opened inode, closing the TOCTOU window entirely. O_NONBLOCK keeps a FIFO
// swapped onto the target from blocking the worker (mirroring the engine's
// openUploadFileNoFollow); a non-symlink replacement node still gets the
// metadata, which stays contained to that local inode, matching the prior
// behaviour.
//
// The O_RDONLY open cannot itself reintroduce a permission wedge: the target is
// always a node the engine just materialised with a readable mode
// (downloadStagePerm for files, 0o755 for fsutil.Mkdir directories) and the
// server-sent perm is applied AFTER the open. The one residual is a REPLAYED
// mkdir SetMeta onto a directory a previous apply already chmodded to an
// owner-unreadable server perm (e.g. 0300) - the open fails EACCES and the op
// stays transient/Stuck-observable, accepted: such a tree is already
// unscannable, and the server itself authored the mode.
func setMetaNoFollow(path string, mtime int64, mode os.FileMode, applyPerm bool) error {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return fmt.Errorf("fsutil: open %s for metadata: %w", path, err)
	}
	defer f.Close()
	if mtime != 0 {
		tv := unix.NsecToTimeval(time.Unix(mtime, 0).UnixNano())
		// Futimes on LINUX is Utimes("/proc/self/fd/N") - x/sys says so in as many words, and
		// exposes no futimens - so this one call needs /proc mounted. In an environment without
		// it (a chroot, a hardened container) every download's promote fails here, forever: the
		// op is transient, the next drain re-fetches the whole file and fails at the same line.
		//
		// Fixing the MECHANISM means utimensat(fd, NULL, …) through a raw syscall with unsafe,
		// which this codebase has no other use of, for a failure only an exotic environment
		// reaches. So the DIAGNOSIS is fixed instead: bare, the error reads "set mtime on
		// <file>: no such file or directory", which points at the file - the one thing that
		// certainly does exist - and tells the operator nothing. Declared in DESIGN §13.
		if err := unix.Futimes(int(f.Fd()), []unix.Timeval{tv, tv}); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return fmt.Errorf("fsutil: set mtime on %s: %w (on Linux this call goes through "+
					"/proc/self/fd; a missing /proc mount fails it while the file itself is fine)", path, err)
			}
			return fmt.Errorf("fsutil: set mtime on %s: %w", path, err)
		}
	}
	if applyPerm {
		if err := f.Chmod(mode); err != nil {
			return fmt.Errorf("fsutil: chmod %s: %w", path, err)
		}
	}
	return nil
}
