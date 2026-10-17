//go:build windows

package engine

import "strings"

// componentHostRepresentable reports whether Windows materialises a path component
// verbatim.
//
// Win32 strips trailing dots and spaces from every component of a path that does not carry
// the `\\?\` prefix - and Go only builds that prefix through fixLongPath, i.e. for over-long
// paths on a host without LongPathsEnabled, never for ordinary ones. The strip is SILENT:
// creating "foo." succeeds and yields "foo", and Lstat("foo.") answers for "foo". There is no
// errno to classify and no way to address such a name, so the engine refuses to represent it
// (see namemangle.go).
//
// "." and ".." are exempt: they are path syntax, not names, and the lexical containment checks
// (filepath.IsLocal, the "."-collapse and canonical-form rejections in containable/abs) own
// them. Reporting them unrepresentable here would reject paths those checks already accept or
// reject on their own, better-stated terms.
//
// A component carrying a character Win32 refuses in a name (<>:"|?* or a control byte) is
// unrepresentable too. That one is not silent - CreateFile fails with ERROR_INVALID_NAME and
// the op dead-letters - but a DSM name like "Report | 2025.pdf" is common, and refusing it here
// skips the event before it is queued, baselined and counted as a dropped op.
func componentHostRepresentable(c string) bool {
	switch c {
	case "", ".", "..":
		return true
	}
	if strings.ContainsFunc(c, win32InvalidNameRune) {
		return false
	}
	switch c[len(c)-1] {
	case '.', ' ':
		return false
	}
	return true
}

// win32InvalidNameRune reports a character Win32 never accepts inside a file name.
func win32InvalidNameRune(r rune) bool {
	return r < 0x20 || strings.ContainsRune(`<>:"|?*`, r)
}
