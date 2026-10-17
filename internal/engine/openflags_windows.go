//go:build windows

package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

// errSymlinkRedirect marks an upload open whose resolved handle does not match the literal
// path under the sync root - proof a symlink (reparse point) along the path redirected it.
// openUploadSource maps it (via pathHasSymlinkComponent) to errUploadSourceGone, so the op is
// reconciled on the next scan rather than wedging; crucially no bytes are ever read.
var errSymlinkRedirect = errors.New("engine: upload source resolved through a symlink")

// errPathVerifyUnsupported marks an upload/hash open that failed because the underlying
// filesystem does not implement GetFinalPathNameByHandle (some SMB/WebDAV redirectors and
// FUSE-style user-mode mounts return ERROR_INVALID_PARAMETER / ERROR_NOT_SUPPORTED). This is
// the one no-follow guard on Windows that CANNOT degrade to a best-effort non-verified success:
// verifyNoSymlinkRedirect's canonical-path comparison IS the symlink-redirect security check
// (there is no per-component O_NOFOLLOW on Windows), so the open must FAIL CLOSED - refusing to
// read a byte it cannot prove stays on the literal path - rather than reading potentially
// cross-view-redirected content. Fail-closed is the recorded decision: the
// residual is diagnosability, not safety, so the sentinel lets the engine surface ONE clear
// once-per-view "uploads disabled" Warn (pathVerifyUnsupported) instead of a per-file Warn-wall
// of opaque errors. A daemon restart re-probes, so a mount that later gains the capability
// recovers.
var errPathVerifyUnsupported = errors.New("engine: filesystem does not support path verification")

// pathVerifyUnsupported reports whether err is the capability-gap sentinel raised when the
// filesystem cannot verify a no-follow open's canonical path (errPathVerifyUnsupported). The
// engine uses it to fire a single once-per-view "uploads disabled" Warn rather than a per-file
// Warn-wall. The Unix build (openflags_unix.go) always reports false: its openat(O_NOFOLLOW)
// walk refuses every symlink at open time and needs no resolve-by-handle post-check that could
// hit this capability gap.
func pathVerifyUnsupported(err error) bool {
	return errors.Is(err, errPathVerifyUnsupported)
}

// openUploadFileNoFollow opens syncRoot/rel for reading and refuses ANY symlink (reparse
// point) along the path - intermediate components included - closing the cross-view TOCTOU
// window race-free, the way the Unix build does with a per-component openat(O_NOFOLLOW) walk.
//
// Windows has no per-component O_NOFOLLOW, and os.Root - while it refuses a link whose target
// ESCAPES the root - still FOLLOWS an in-root relative symlink into a sibling view, so a
// component swapped for such a link after the pre-open check (abs/noSymlinkEscape) could
// redirect the read into another view. The privilege barrier is not a sufficient
// defence: Windows Developer Mode lets a non-admin create symlinks. Close it by opening through
// os.Root (which gives component-relative addressing and refuses escaping links) and then
// verifying race-free that the handle we ACTUALLY opened resolves to the literal path under the
// root: if any component was a symlink redirect, the canonical path of the open handle diverges
// from the expected one and we refuse, never having read a byte. O_NOFOLLOW is intentionally not
// passed - os.Root ignores it and would follow the link regardless, so the post-open canonical
// check is the real guard.
func openUploadFileNoFollow(syncRoot, rel string) (*os.File, error) {
	root, err := os.OpenRoot(syncRoot)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	f, err := root.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	if verr := verifyNoSymlinkRedirect(syncRoot, rel, f); verr != nil {
		_ = f.Close()
		return nil, verr
	}
	return f, nil
}

// verifyNoSymlinkRedirect confirms the opened handle f resolves to the literal syncRoot/rel,
// with no symlink redirect along the way. It compares the canonical final path of f against the
// canonical final path of the sync root joined with rel; both come from GetFinalPathNameByHandle
// so their volume prefix and normalisation are consistent. A symlink anywhere on the path would
// make f's canonical path diverge (e.g. resolve under a sibling view), which fails the compare.
func verifyNoSymlinkRedirect(syncRoot, rel string, f *os.File) error {
	rootDir, err := os.Open(syncRoot)
	if err != nil {
		return err
	}
	defer func() { _ = rootDir.Close() }()
	rootCanon, err := finalPathByHandle(windows.Handle(rootDir.Fd()))
	if err != nil {
		return err
	}
	fileCanon, err := finalPathByHandle(windows.Handle(f.Fd()))
	if err != nil {
		return err
	}
	// rel is OS-separated, containment-validated (no leading separator, "." or ".."), so a
	// plain join under the canonical root is the expected literal path. Trim any trailing
	// separator on the root (a drive root canonicalises to e.g. \\?\C:\) so the join never
	// doubles it. Windows paths are case-insensitive, so compare with EqualFold.
	want := strings.TrimRight(rootCanon, `\`) + `\` + rel
	if !strings.EqualFold(fileCanon, want) {
		return &os.PathError{Op: "openat", Path: filepath.Join(syncRoot, rel), Err: errSymlinkRedirect}
	}
	return nil
}

// GetFinalPathNameByHandle flags (not exported by golang.org/x/sys/windows). Both are zero:
// FILE_NAME_NORMALIZED requests the normalised path, VOLUME_NAME_DOS the drive-letter form -
// exactly the canonical comparison key verifyNoSymlinkRedirect needs.
const (
	fileNameNormalized = 0x0
	volumeNameDOS      = 0x0
)

// finalPathByHandle returns the fully-resolved (symlinks followed, normalised, DOS-volume) path
// the handle h refers to, via GetFinalPathNameByHandle. It grows the buffer and retries when the
// path is longer than the initial guess.
//
// Fail-closed decision: a filesystem that does not implement this call
// (some network redirectors and FUSE-style mounts return ERROR_INVALID_PARAMETER /
// ERROR_NOT_SUPPORTED) makes verifyNoSymlinkRedirect - the ONLY symlink-redirect guard on the
// Windows upload path - impossible to run. That gap is tagged with errPathVerifyUnsupported and
// propagated as a hard error, NOT degraded to a non-verified success: the verification is a
// security check, so an open it cannot prove stays on the literal path is refused outright (no
// byte read). The engine turns that sentinel into one clear once-per-view diagnostic; uploads on
// such a view are disabled until the operator moves the sync root to a supporting filesystem.
func finalPathByHandle(h windows.Handle) (string, error) {
	buf := make([]uint16, windows.MAX_PATH)
	for {
		// buf is either windows.MAX_PATH long or exactly the uint32 length the previous
		// call reported, so len(buf) always fits a uint32 by construction.
		bufLen := uint32(len(buf)) //nolint:gosec // len(buf) is MAX_PATH or a prior uint32 n
		n, err := windows.GetFinalPathNameByHandle(h, &buf[0], bufLen,
			fileNameNormalized|volumeNameDOS)
		if err != nil {
			if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_NOT_SUPPORTED) {
				return "", fmt.Errorf("%w: %w", errPathVerifyUnsupported, err)
			}
			return "", err
		}
		// A return larger than the buffer is the required length (including the null
		// terminator): grow and retry. Otherwise n is the resolved length, sans terminator.
		if int(n) > len(buf) {
			buf = make([]uint16, n)
			continue
		}
		return windows.UTF16ToString(buf[:n]), nil
	}
}
