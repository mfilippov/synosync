package config

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/mfilippov/synosync/internal/normpath"
)

// ExcludedPath names one server path that is not synced, with everything under it.
//
// The path is view-relative, slash-separated, NFC, without a leading slash ("Photos",
// "Documents/Archive"): the same spelling the engine keys baselines by, so one comparison
// decides it everywhere. Matching is by normpath.Key, the engine's own path identity: NFC/NFD
// spellings always match, and case-only variants match where the sync root folds case (the
// engine probes it at start), so an exclusion covers exactly the names the disk cannot tell
// apart from it.
type ExcludedPath struct {
	// ViewID is the server's view id, the same key Selected matches on.
	ViewID uint64 `json:"view_id"`
	// ViewName is what the view was called when the path was excluded. Display only, as
	// SelectedView.Name: it may be stale after a rename, and nothing matches on it.
	ViewName string `json:"view_name,omitempty"`
	// Path is the excluded view-relative path.
	Path string `json:"path"`
}

// NormalizeExcludedPath turns what a user typed ("/Photos/", "Photos//2024") into the stored
// spelling, or reports why it cannot name a subtree.
func NormalizeExcludedPath(p string) (string, error) {
	slashed := strings.ReplaceAll(p, "\\", "/")
	// Checked before cleaning: Clean of a rooted path silently drops a leading "..", which
	// would turn "../x" into "x" instead of refusing it.
	for _, c := range strings.Split(slashed, "/") {
		if c == ".." {
			return "", fmt.Errorf("config: excluded path %q escapes the view", p)
		}
	}
	clean := strings.TrimPrefix(path.Clean("/"+slashed), "/")
	if clean == "" || clean == "." {
		return "", errors.New("config: the view root cannot be excluded; deselect the view instead")
	}
	return normpath.Normalize(clean), nil
}

// Excludes reports whether rel (view-relative, slash-separated) is ex or lies under it.
func (ex ExcludedPath) Excludes(viewID uint64, rel string) bool {
	if ex.ViewID != viewID {
		return false
	}
	k, r := normpath.Key(ex.Path), normpath.Key(rel)
	return r == k || strings.HasPrefix(r, k+"/")
}

// validateExcluded checks the stored exclusions: each a normalised subtree path of a view, none
// listed twice, none inside another (the inner one would be dead weight that outlives the outer).
func validateExcluded(excluded []ExcludedPath) error {
	for i, ex := range excluded {
		if ex.ViewID == 0 {
			return fmt.Errorf("config: views.excluded %q has no view id", ex.Path)
		}
		norm, err := NormalizeExcludedPath(ex.Path)
		if err != nil {
			return err
		}
		if norm != ex.Path {
			return fmt.Errorf("config: views.excluded %q is not in its normal form %q", ex.Path, norm)
		}
		for _, other := range excluded[:i] {
			if other.Excludes(ex.ViewID, ex.Path) || ex.Excludes(other.ViewID, other.Path) {
				return fmt.Errorf("config: views.excluded %q overlaps %q", ex.Path, other.Path)
			}
		}
	}
	return nil
}

// IsExcluded reports whether rel in view viewID is under any exclusion.
func (v Views) IsExcluded(viewID uint64, rel string) bool {
	for _, ex := range v.Excluded {
		if ex.Excludes(viewID, rel) {
			return true
		}
	}
	return false
}

// NodeKind is what a path names, when the caller knows.
type NodeKind uint8

const (
	// KindUnknown is a node whose kind was not reported. A files-only rule never applies to it:
	// a directory called "tmp" seen through a sparse move must not be mistaken for a file.
	KindUnknown NodeKind = iota
	// KindFile is a regular file.
	KindFile
	// KindDir is a directory.
	KindDir
)

// ExcludedName is a rule on one path component: a case-insensitive glob (path.Match syntax)
// matched against every name in a path, so a matching directory excludes its whole subtree.
// FilesOnly limits it to the last component of a path known to be a file - the stock client's
// suffix and "~" rules, which never hide a directory.
type ExcludedName struct {
	// Pattern is the glob, e.g. "thumbs.db", "*.tmp", "~*".
	Pattern string `json:"pattern"`
	// FilesOnly applies the rule to files only.
	FilesOnly bool `json:"files_only,omitempty"`
}

// SystemNames are excluded always, in every view, and cannot be removed: the NAS treats them
// as its own (thumbnails, recycle bin, snapshots, Drive's working state), so a local folder of
// that name pushed to the server would mix with them. The stock client holds them as fixed
// "system rules" too (docs/ENGINE.md, exclude.go).
var SystemNames = []string{"@tmp", "@eaDir", ".SynologyWorkingDirectory", "#recycle", "#snapshot", "@sharebin"}

// DefaultExcludedPaths are excluded in the home view of a new pairing, as the stock client's
// new-task wizard does (it leaves the phone backups and Drive backups out by exact root name).
var DefaultExcludedPaths = []string{"Photos", "Backup"}

// DefaultExcludedNames are the name rules a new pairing starts with: the stock client's
// operating-system leftovers and its temporary-file suffixes. Each can be removed.
func DefaultExcludedNames() []ExcludedName {
	out := []ExcludedName{}
	for _, n := range []string{"desktop.ini", "thumbs.db", ".ds_store", "Icon\r", "$Recycle.Bin",
		"System Volume Information", "Program Files", "Program Files (x86)", "ProgramData"} {
		out = append(out, ExcludedName{Pattern: n})
	}
	for _, n := range []string{"*.tmp", "*.temp", "*.swp", "*.lnk", "*.pst", "tmp", "temp", "~*"} {
		out = append(out, ExcludedName{Pattern: n, FilesOnly: true})
	}
	return out
}

// NormalizeNamePattern validates a name rule as typed: one component, a well-formed glob.
func NormalizeNamePattern(p string) (string, error) {
	p = normpath.Normalize(p)
	if p == "" || p == "." || p == ".." || strings.ContainsAny(p, "/\\") {
		return "", fmt.Errorf("config: name rule %q must be a single file or folder name (a glob like *.tmp is fine)", p)
	}
	if _, err := path.Match(p, ""); err != nil {
		return "", fmt.Errorf("config: name rule %q is not a valid pattern: %w", p, err)
	}
	return p, nil
}

// matchesName reports whether one component name of the given kind falls under the rule.
func (n ExcludedName) matchesName(name string, kind NodeKind) bool {
	if n.FilesOnly && kind != KindFile {
		return false
	}
	ok, _ := path.Match(strings.ToLower(n.Pattern), strings.ToLower(name))
	return ok
}

// Matches reports whether any component of rel falls under the rule. Every component but the
// last is a directory; the last has the given kind.
func (n ExcludedName) Matches(rel string, kind NodeKind) bool {
	parts := strings.Split(rel, "/")
	for i, c := range parts {
		k := KindDir
		if i == len(parts)-1 {
			k = kind
		}
		if c != "" && n.matchesName(c, k) {
			return true
		}
	}
	return false
}

// NameExcluded reports whether a component of rel is a system name or falls under a name rule.
func (v Views) NameExcluded(rel string, kind NodeKind) bool {
	for _, c := range strings.Split(rel, "/") {
		for _, s := range SystemNames {
			if strings.EqualFold(c, s) {
				return true
			}
		}
	}
	for _, n := range v.ExcludedNames {
		if n.Matches(rel, kind) {
			return true
		}
	}
	return false
}

// Excludes reports whether rel in view viewID is out of scope, by path or by name.
func (v Views) Excludes(viewID uint64, rel string, kind NodeKind) bool {
	return v.IsExcluded(viewID, rel) || v.NameExcluded(rel, kind)
}

// validateNames checks the stored name rules: each well-formed, none listed twice.
func validateNames(names []ExcludedName) error {
	seen := make(map[ExcludedName]bool, len(names))
	for _, n := range names {
		norm, err := NormalizeNamePattern(n.Pattern)
		if err != nil {
			return err
		}
		if norm != n.Pattern {
			return fmt.Errorf("config: name rule %q is not in its normal form %q", n.Pattern, norm)
		}
		k := ExcludedName{Pattern: strings.ToLower(n.Pattern), FilesOnly: n.FilesOnly}
		if seen[k] {
			return fmt.Errorf("config: name rule %q is listed twice", n.Pattern)
		}
		seen[k] = true
	}
	return nil
}
