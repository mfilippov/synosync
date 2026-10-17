//go:build !windows

package engine

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// osDeviceID returns the filesystem device id (st_dev) the path resides on. The empty-scan
// debounce records this for a healthy view root and re-reads it on an empty scan, so a mount
// that dropped or swapped underneath the root - which presents as an ordinary empty directory
// - is detected by its changed device id and treated as unavailable instead of being counted
// toward a whole-view delete. It follows symlinks (unix.Stat, not Lstat): the
// no-symlink-escape guards run elsewhere, and the recorded view root is already a real
// directory, so following here only resolves the actual mount the content lives on.
//
// Blind spot (accepted): st_dev identifies the FILESYSTEM, not the
// mount, so a SAME-DEVICE mount change is invisible to it - unmounting a bind mount (or a
// btrfs subvolume mounted from the same device) that exposed one subtree at the view root
// leaves the underlying directory of the SAME filesystem visible there, with an identical
// st_dev and different (usually empty) content. The empty-scan debounce still guards that
// case - it is the reason the debounce exists and is not replaced by the device check - but
// the device signal adds nothing, so a same-device swap converges on the debounce's
// threshold alone. Linux statx(STATX_MNT_ID) would give a true mount identity and is the
// natural future fix; it is not adopted here because it is Linux-only (macOS has no
// equivalent per-mount id in stat, so the guard would become asymmetric across the release
// matrix) and the residual failure direction is bounded by the debounce. See DESIGN §13.
func osDeviceID(path string) (uint64, error) {
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		return 0, fmt.Errorf("engine: stat device id %q: %w", path, err)
	}
	// st.Dev is int32 on some platforms (Darwin) and uint64 on others (Linux); the unsigned
	// widening is correct for both - only the bit pattern matters as an opaque identity key.
	// (unconvert flags the no-op cast on Linux, but it is load-bearing on Darwin; gosec G115
	// flags the widening on Darwin, where a negative st.Dev sign-extends - harmless, because
	// the result is never arithmetic or a length, only compared against a remembered value,
	// and the mapping stays injective. Both directives are needed because each fires in only
	// ONE build context - the class `make lint-cross` exists to surface.)
	return uint64(st.Dev), nil //nolint:unconvert,gosec // platform-dependent: st.Dev is int32 on Darwin
}
