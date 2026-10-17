//go:build !windows

package engine

import (
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openUploadFileNoFollow opens syncRoot/rel for reading while refusing to traverse OR open
// ANY symlink along the path - intermediate components included. It walks the path one
// component at a time with openat(O_NOFOLLOW), each relative to the previous directory's
// descriptor, so no component can be a symlink that redirects the open elsewhere.
//
// This is stronger than opening through os.Root, which blocks only links whose target
// escapes the sync root but FOLLOWS an in-root relative symlink: a component swapped for a
// link into a SIBLING view still sits within the root, so os.Root would read that other
// view's bytes. openat with O_NOFOLLOW fails closed (ELOOP) on every symlink,
// in-root or not, intermediate or final, so the read can only ever reach the literal path.
//
// The syscalls go through golang.org/x/sys/unix rather than the standard syscall package:
// syscall.Openat is Linux-only, so a syscall-based opener fails to compile on Darwin and
// FreeBSD. unix.Openat is defined on every Unix target.
//
// rel is the OS-separated, already containment-validated (filepath.IsLocal in abs) path
// under the root, so it carries no absolute, "." or ".." components. The sync root itself
// is opened without O_NOFOLLOW - reaching it through a symlink is the operator's own choice
// and stays contained (matching noSymlinkEscape, which also does not inspect the root). The
// final component is opened O_NONBLOCK so a non-regular node (e.g. a FIFO substituted for
// the file) returns immediately for the caller's fstat type check instead of blocking the
// worker; O_NONBLOCK has no effect on a regular-file read. Every fd is O_CLOEXEC.
func openUploadFileNoFollow(syncRoot, rel string) (*os.File, error) {
	dirfd, err := unix.Open(syncRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: syncRoot, Err: err}
	}

	comps := make([]string, 0)
	for _, c := range strings.Split(rel, string(filepath.Separator)) {
		if c != "" {
			comps = append(comps, c)
		}
	}
	if len(comps) == 0 {
		_ = unix.Close(dirfd)
		// rel is always a non-empty file path under the root; an empty walk means the source
		// is the root itself, which is never an upload source.
		return nil, &os.PathError{Op: "open", Path: syncRoot, Err: unix.EISDIR}
	}

	cur := syncRoot
	for i, c := range comps {
		last := i == len(comps)-1
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if last {
			flags |= unix.O_NONBLOCK
		} else {
			flags |= unix.O_DIRECTORY
		}
		fd, oerr := unix.Openat(dirfd, c, flags, 0)
		_ = unix.Close(dirfd) // done with the parent directory descriptor
		cur = filepath.Join(cur, c)
		if oerr != nil {
			return nil, &os.PathError{Op: "openat", Path: cur, Err: oerr}
		}
		if last {
			return os.NewFile(uintptr(fd), cur), nil
		}
		dirfd = fd
	}
	// Unreachable: the final component always returns inside the loop.
	_ = unix.Close(dirfd)
	return nil, &os.PathError{Op: "open", Path: cur, Err: unix.EINVAL}
}

// pathVerifyUnsupported is always false on Unix. The openat(O_NOFOLLOW) walk above refuses
// every symlink at open time, so there is no post-open resolve-by-handle verification that
// could hit a filesystem-capability gap - unlike the Windows build, whose only symlink-redirect
// guard is a GetFinalPathNameByHandle canonical-path comparison that some redirector/FUSE
// mounts cannot serve (errPathVerifyUnsupported).
func pathVerifyUnsupported(error) bool { return false }
