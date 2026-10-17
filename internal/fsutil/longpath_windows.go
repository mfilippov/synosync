//go:build windows

package fsutil

import (
	"path/filepath"
	"strings"
)

// extendedPathThreshold is the path length at or above which a Win32 call needs
// the extended-length form. It mirrors the stdlib's own trigger (os.fixLongPath
// leaves anything shorter alone): MAX_PATH is 260, and a directory path must
// still have room for a 12-character 8.3 name plus a separator, so 248 is the
// conservative point past which a raw Win32 path can be rejected.
const extendedPathThreshold = 248

// ExtendedPath returns path in the `\\?\` extended-length form when it is long
// enough for Win32's MAX_PATH limit to reject it, and returns it unchanged
// otherwise.
//
// The stdlib applies this itself (os.fixLongPath) for every path that reaches a
// syscall through the os package, which is why this client materialises trees
// deeper than 260 characters through os.MkdirAll/os.Rename on a default-registry
// Windows even with LongPathsEnabled unset - but a DIRECT Win32 call built from
// windows.UTF16PtrFromString bypasses it and fails on exactly the trees the os
// package created. Every direct call in this package routes
// its paths through here, and it is exported for the handful outside it
// (engine/statdev_windows.go's CreateFile, engine/shortname_windows.go's
// GetLongPathName) rather than copied: the transformation is subtle enough that a
// second copy would drift.
//
// The transformation is deliberately conditional on length: the extended form
// also disables Win32's own path normalisation (`.`/`..` resolution, trailing
// dot/space stripping), so applying it unconditionally would change the semantics
// of every short path too. Only an absolute path can be prefixed - a relative one
// is made absolute first, since `\\?\` requires a fully qualified path - and a UNC
// path takes the `\\?\UNC\` form. Anything that cannot be expressed (a failed Abs,
// a shape that is neither drive-letter nor UNC) is returned unchanged, leaving the
// caller with the same MAX_PATH exposure it had before rather than a broken path.
func ExtendedPath(path string) string {
	if len(path) < extendedPathThreshold {
		return path
	}
	if strings.HasPrefix(path, `\\?\`) || strings.HasPrefix(path, `\??\`) {
		return path
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return path
		}
		path = abs
	}
	// The extended form is passed to the object manager verbatim, so it must
	// already be normalised: a `.`/`..` component or a forward slash would be
	// taken literally instead of resolved.
	path = filepath.Clean(path)
	switch {
	case strings.HasPrefix(path, `\\`):
		return `\\?\UNC\` + strings.TrimPrefix(path, `\\`)
	case len(path) >= 3 && path[1] == ':' && path[2] == '\\':
		return `\\?\` + path
	default:
		return path
	}
}
