//go:build !windows

package engine

// hostAliasesShortNames records that a Unix-like host stores exactly one name per directory
// entry: there is no auto-generated 8.3 alias, so a path addresses the entity it spells and the
// gate in shortname.go is a no-op (a test may still wire the e.longPath seam to drive it).
const hostAliasesShortNames = false

// osLongPathName returns name unchanged: the host has no second spelling to resolve.
func osLongPathName(name string) (string, error) { return name, nil }
