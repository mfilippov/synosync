package watcher

import (
	"path/filepath"
	"strings"
)

// Path noise filters decide whether a filesystem event should be suppressed
// before it contributes to a wake. They exist purely to avoid spurious wakes
// from self-induced writes - the engine's executor materialises downloads
// through a hidden staging temp, and the keep-both resolver pushes a "_Conflict"
// copy - and from the transient temp/swap files editors and downloaders create.
//
// The filters are deliberately conservative: a wake only schedules a localScan,
// and the authoritative diff is still scanView + MD4 driven by the engine's
// periodic catch-up scan (DESIGN §7). So a false-negative filter (failing to
// wake on a real change because its name resembles noise) at worst adds latency
// until that periodic scan runs - it never loses data. We therefore err toward
// suppressing recognisably self-induced/transient names and let everything else
// through.
//
// gitignore-style user-facing ignore patterns are out of scope for Phase 7 and
// intentionally not implemented here.

// conflictMarker is the keep-both copy marker the engine's conflict resolver
// inserts before the final extension of a divergent file's base name
// (see internal/engine/conflict.go conflictCopyName).
const conflictMarker = "_Conflict"

// tempSuffixes are case-insensitive base-name suffixes that mark a transient
// partial/temp/editor-swap file rather than durable user content.
var tempSuffixes = []string{
	".tmp",
	".temp",
	".swp", // vim swap
	".swo", // vim swap (second buffer)
	".swx", // vim swap (third buffer)
	".part",
	".partial",
	".crdownload", // Chrome partial download
	".download",   // Safari partial download
}

// shouldIgnore reports whether a filesystem event for path is noise that must
// not contribute to a wake. It matches on the final path component only.
func shouldIgnore(path string) bool {
	base := filepath.Base(path)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return false
	}
	return isStagingName(base) || isConflictCopyName(base) || isTempName(base)
}

// isStagingName mirrors the engine executor's download staging-temp naming
// (os.CreateTemp(dir, "."+base+".dl-*")): a hidden file whose name ends in the
// ".dl-" marker followed by os.CreateTemp's decimal suffix. The match is precise
// - the marker must be followed by one or more digits ending the name - so a
// real user file that merely contains ".dl-" is not suppressed.
func isStagingName(base string) bool {
	if !strings.HasPrefix(base, ".") {
		return false
	}
	i := strings.LastIndex(base, ".dl-")
	if i < 0 {
		return false
	}
	return allDigits(base[i+len(".dl-"):])
}

// isConflictCopyName reports whether base looks like a keep-both conflict copy:
// the engine inserts the "_Conflict" marker immediately before the final
// extension, so the base name with its extension stripped ends in the marker.
// On a same-second name collision the resolver appends a "_<n>" counter after
// the marker (engine.conflictCopyNameN), so a trailing "_<digits>" counter is
// stripped before matching - "report_host_..._Conflict_1.docx" is recognised
// as self-induced too.
func isConflictCopyName(base string) bool {
	name := base
	if ext := filepath.Ext(base); ext != "" && len(ext) < len(base) {
		name = base[:len(base)-len(ext)]
	}
	if i := strings.LastIndex(name, "_"); i > 0 && allDigits(name[i+1:]) {
		name = name[:i]
	}
	return strings.HasSuffix(name, conflictMarker)
}

// allDigits reports whether s is non-empty and consists solely of ASCII digits.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// isTempName reports whether base carries a recognised transient temp/partial/
// editor-swap suffix or is a well-known editor probe file.
func isTempName(base string) bool {
	// emacs/gedit and many editors append a trailing "~" to backup files.
	if strings.HasSuffix(base, "~") {
		return true
	}
	// vim writes a transient file literally named "4913" to test writability.
	if base == "4913" {
		return true
	}
	lower := strings.ToLower(base)
	for _, s := range tempSuffixes {
		if strings.HasSuffix(lower, s) {
			return true
		}
	}
	return false
}
