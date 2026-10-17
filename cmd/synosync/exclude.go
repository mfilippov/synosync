package main

// The `exclude` subcommand is selective sync: server paths that are not synced, each with its
// whole subtree. The engine side is internal/engine/exclude.go; this file edits the list and
// does the store-side half of each change, with the daemon stopped (it needs the store lock,
// as `config set` does).

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// excludeHelp is the `exclude` help page.
var excludeHelp = helpSpec{
	name:    "exclude",
	summary: "choose server folders that are not synced",
	usage: []string{
		"synosync exclude list",
		"synosync exclude add [--view NAME] PATH...",
		"synosync exclude remove [--view NAME] PATH...",
		"synosync exclude add --name [--files] PATTERN...",
		"synosync exclude remove --name PATTERN...",
		"synosync exclude defaults [--view NAME]",
	},
	describe: `Keeps server paths out of the sync, each with everything under it. An
excluded path is left alone in both directions: nothing is downloaded from it,
nothing under its local copy is uploaded, and nothing on either side is deleted
because of it. PATH is relative to the view, with or without a leading slash.

Adding an exclusion never deletes anything. What was already synced stays on
disk and simply stops being tracked; remove it yourself if you want the space
back. Queued transfers under the path are cancelled.

Removing an exclusion brings the path back: the daemon walks that subtree on
the server, compares it with what is on disk, and downloads what is missing.
A local file that differs from the server's is kept as a _Conflict copy rather
than overwritten.

With --name the arguments are NAME RULES instead of paths: a file or folder
name, or a pattern like *.tmp, matched case-insensitively against every name in
every view; a matching folder takes everything under it. --files limits a rule
to files. A new pairing starts with the rules the official client uses
(desktop.ini, thumbs.db, .DS_Store, *.tmp, ~* and similar) and with the home
view's Photos and Backup excluded; "defaults" adds any of them that are
missing. The NAS's own names (@eaDir, #recycle, #snapshot, @sharebin, @tmp,
.SynologyWorkingDirectory) are always excluded and are not listed as rules.

Removing a name rule walks the whole view again, since the files it hid can be
anywhere: on a large NAS that takes a while.

--view is needed only when more than one view syncs. add, remove and defaults
need the daemon stopped; list works either way.`,
	examples: []string{
		"synosync exclude add Photos",
		"synosync exclude add --view home Photos/MobileBackup Movies",
		"synosync exclude remove Photos",
		"synosync exclude add --name --files '*.bak'",
		"synosync exclude remove --name '*.lnk'",
		"synosync exclude defaults",
		"synosync exclude list",
	},
}

// cmdExclude implements the `exclude` subcommand.
func cmdExclude(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		writeUsageHint(stderr, excludeHelp)
		return 2
	}
	if isHelpArg(args[0]) {
		writeHelp(stdout, excludeHelp, nil)
		return 0
	}
	switch args[0] {
	case "list":
		if code, ok := rejectExtraArgs(excludeHelp, args[1:], stderr); !ok {
			return code
		}
		cfg, code := loadConfigForGet(stderr)
		if code != 0 {
			return code
		}
		printExclusions(stdout, cfg)
		return 0
	case "add", "remove":
		return excludeEdit(args[0] == "add", args[1:], stdout, stderr)
	case "defaults":
		return excludeDefaults(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "synosync: unknown exclude action %q, want \"list\", \"add\", \"remove\" or \"defaults\"\n", args[0])
		writeUsageHint(stderr, excludeHelp)
		return 2
	}
}

// excludeEdit adds or removes exclusions.
func excludeEdit(add bool, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("exclude", flag.ContinueOnError)
	viewName := fs.String("view", "", "the view the paths belong to (needed when several sync)")
	byName := fs.Bool("name", false, "the arguments are name rules (a name or a pattern like *.tmp), not paths")
	filesOnly := fs.Bool("files", false, "with --name: the rule applies to files only")
	if code, ok := parseFlags(fs, excludeHelp, args, stdout, stderr); !ok {
		return code
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(stderr, "synosync: exclude: name at least one PATH (or PATTERN with --name)")
		writeUsageHint(stderr, excludeHelp)
		return 2
	}
	if *filesOnly && !*byName {
		fmt.Fprintln(stderr, "synosync: exclude: --files goes with --name")
		return 2
	}
	if *byName {
		return excludeNames(add, *filesOnly, fs.Args(), stdout, stderr)
	}
	paths := make([]string, 0, fs.NArg())
	for _, a := range fs.Args() {
		p, err := config.NormalizeExcludedPath(a)
		if err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 2
		}
		paths = append(paths, p)
	}

	st, cfg, code := openConfigForCommand(stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = st.Close() }()
	viewID, name, err := resolveExcludeView(st, cfg, *viewName)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if add {
		return excludeAdd(st, cfg, viewID, name, paths, stdout, stderr)
	}
	return excludeRemove(st, cfg, viewID, name, paths, stdout, stderr)
}

// excludeAdd records the exclusions and then forgets each subtree in the store.
//
// The config is written FIRST. Forgetting first and failing before the config write would leave
// the subtree untracked but still synced: the next scan would read every file in it as a local
// create and push it, colliding with the server's copies. Written first, a failure before the
// drop leaves an excluded subtree whose stale baselines the scan protects; rerunning the command
// completes the drop, since it runs whether or not the path was already excluded.
func excludeAdd(st *store.Store, cfg config.Config, viewID uint64, name string, paths []string, stdout, stderr io.Writer) int {
	next := slices.Clone(cfg.Views.Excluded)
	for _, p := range paths {
		if cfg.Views.IsExcluded(viewID, p) {
			continue
		}
		// A new path that covers existing exclusions replaces them.
		next = slices.DeleteFunc(next, func(ex config.ExcludedPath) bool {
			return config.ExcludedPath{ViewID: viewID, Path: p}.Excludes(ex.ViewID, ex.Path)
		})
		next = append(next, config.ExcludedPath{ViewID: viewID, ViewName: name, Path: p})
		cfg.Views.Excluded = next
	}
	cfg.Views.Excluded = next
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	for _, p := range paths {
		match := func(rel string) bool { return config.ExcludedPath{ViewID: viewID, Path: p}.Excludes(viewID, rel) }
		baselines, ops, err := st.DropSubtree(viewID, func(rel string, _ bool) bool { return match(rel) })
		if err != nil {
			fmt.Fprintf(stderr, "synosync: forget %s: %s (the exclusion is saved; run the command again)\n", p, userErr(err))
			return 1
		}
		if err := updateRescans(st, viewID, func(r []string) []string {
			return slices.DeleteFunc(r, match)
		}); err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 1
		}
		fmt.Fprintf(stdout, "excluded %s:/%s (%d tracked entr(ies) forgotten, %d queued op(s) cancelled; files on disk are kept)\n",
			name, p, baselines, ops)
	}
	return 0
}

// excludeRemove lifts exclusions and queues a rescan of each path.
//
// The rescan is recorded FIRST: the engine keeps a rescan of a still-excluded path waiting, so a
// failure before the config write is completed by rerunning the command. The other order could
// lift the exclusion with no rescan, and the subtree would never download - the event log reads
// forward, past everything that happened while it was excluded.
func excludeRemove(st *store.Store, cfg config.Config, viewID uint64, name string, paths []string, stdout, stderr io.Writer) int {
	next := slices.Clone(cfg.Views.Excluded)
	var lifted []string
	for _, p := range paths {
		before := len(next)
		next = slices.DeleteFunc(next, func(ex config.ExcludedPath) bool {
			return ex.ViewID == viewID && normpath.Key(ex.Path) == normpath.Key(p)
		})
		if len(next) == before {
			if cfg.Views.IsExcluded(viewID, p) {
				fmt.Fprintf(stderr, "synosync: %s:/%s is inside a larger exclusion; remove that one instead\n", name, p)
			} else {
				fmt.Fprintf(stderr, "synosync: %s:/%s is not excluded\n", name, p)
			}
			return 1
		}
		lifted = append(lifted, p)
	}
	if err := updateRescans(st, viewID, func(r []string) []string { return store.MergeRescans(r, lifted) }); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	cfg.Views.Excluded = next
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	for _, p := range lifted {
		fmt.Fprintf(stdout, "no longer excluded: %s:/%s (the daemon fetches it on its next cycle)\n", name, p)
	}
	return 0
}

// updateRescans rewrites the view's pending rescans. A view the store does not hold yet has not
// synced at all, so its first sync covers everything and there is nothing to record.
func updateRescans(st *store.Store, viewID uint64, edit func([]string) []string) error {
	v, err := st.GetView(viewID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load view %d: %w", viewID, err)
	}
	if !v.InitialSyncDone {
		return nil
	}
	v.Rescan = edit(v.Rescan)
	if err := st.PutView(viewID, v); err != nil {
		return fmt.Errorf("save view %d: %w", viewID, err)
	}
	return nil
}

// resolveExcludeView picks the view the paths belong to: the one named by --view, or the only
// view that syncs.
func resolveExcludeView(st *store.Store, cfg config.Config, want string) (uint64, string, error) {
	known, err := st.ListViews()
	if err != nil {
		return 0, "", fmt.Errorf("list known views: %w", err)
	}
	var syncing []store.ListView
	for _, v := range known {
		if cfg.Views.Syncs(v.ViewID) {
			syncing = append(syncing, v)
		}
	}
	if len(syncing) == 0 && len(cfg.Views.Selected) == 1 {
		// Paired but the daemon has not enumerated the views yet: the selection names it.
		sv := cfg.Views.Selected[0]
		if want == "" || normpath.Key(want) == normpath.Key(sv.Name) {
			return sv.ID, sv.Name, nil
		}
	}
	if want == "" {
		switch len(syncing) {
		case 1:
			return syncing[0].ViewID, syncing[0].Name, nil
		case 0:
			return 0, "", errors.New("no views are known yet; run the daemon once so it can enumerate them")
		default:
			names := make([]string, 0, len(syncing))
			for _, v := range syncing {
				names = append(names, v.Name)
			}
			return 0, "", fmt.Errorf("several views sync (%s); name one with --view", strings.Join(names, ", "))
		}
	}
	for _, v := range syncing {
		if normpath.Key(v.Name) == normpath.Key(want) {
			return v.ViewID, v.Name, nil
		}
	}
	return 0, "", fmt.Errorf("no syncing view is called %q", want)
}

// excludeNames adds or removes name rules. They apply in every view, so the store-side half runs
// over every view the store knows, in the same orders excludeAdd and excludeRemove use and for
// the same reasons: the config before the drop on add, the rescan before the config on remove.
func excludeNames(add, filesOnly bool, args []string, stdout, stderr io.Writer) int {
	rules := make([]config.ExcludedName, 0, len(args))
	for _, a := range args {
		p, err := config.NormalizeNamePattern(a)
		if err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 2
		}
		rules = append(rules, config.ExcludedName{Pattern: p, FilesOnly: filesOnly})
	}
	st, cfg, code := openConfigForCommand(stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = st.Close() }()
	if add {
		return addNameRules(st, cfg, rules, stdout, stderr)
	}
	return removeNameRules(st, cfg, rules, stdout, stderr)
}

// addNameRules records the rules missing from cfg and forgets what they cover in every view.
func addNameRules(st *store.Store, cfg config.Config, rules []config.ExcludedName, stdout, stderr io.Writer) int {
	for _, r := range rules {
		if !slices.ContainsFunc(cfg.Views.ExcludedNames, func(n config.ExcludedName) bool { return sameRule(n, r) }) {
			cfg.Views.ExcludedNames = append(cfg.Views.ExcludedNames, r)
		}
	}
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	views, err := st.ListViews()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: list known views: %s (the rules are saved; run the command again)\n", userErr(err))
		return 1
	}
	var baselines, ops int
	for _, v := range views {
		b, o, err := st.DropSubtree(v.ViewID, func(rel string, isDir bool) bool {
			kind := config.KindFile
			if isDir {
				kind = config.KindDir
			}
			for _, r := range rules {
				if r.Matches(rel, kind) {
					return true
				}
			}
			return false
		})
		if err != nil {
			fmt.Fprintf(stderr, "synosync: forget matching entries in view %d: %s (the rules are saved; run the command again)\n", v.ViewID, userErr(err))
			return 1
		}
		baselines, ops = baselines+b, ops+o
	}
	for _, r := range rules {
		fmt.Fprintf(stdout, "excluded name %s\n", describeRule(r))
	}
	fmt.Fprintf(stdout, "%d tracked entr(ies) forgotten, %d queued op(s) cancelled; files on disk are kept\n", baselines, ops)
	return 0
}

// removeNameRules lifts the rules and queues a rescan of every synced view, recorded first.
func removeNameRules(st *store.Store, cfg config.Config, rules []config.ExcludedName, stdout, stderr io.Writer) int {
	next := slices.Clone(cfg.Views.ExcludedNames)
	for _, r := range rules {
		before := len(next)
		// --files is not needed to name a rule: removing "*.tmp" removes it in either form.
		next = slices.DeleteFunc(next, func(n config.ExcludedName) bool { return strings.EqualFold(n.Pattern, r.Pattern) })
		if len(next) == before {
			if slices.ContainsFunc(config.SystemNames, func(s string) bool { return strings.EqualFold(s, r.Pattern) }) {
				fmt.Fprintf(stderr, "synosync: %s is one of the NAS's own names and is always excluded\n", r.Pattern)
			} else {
				fmt.Fprintf(stderr, "synosync: no name rule %s\n", r.Pattern)
			}
			return 1
		}
	}
	views, err := st.ListViews()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: list known views: %s\n", userErr(err))
		return 1
	}
	for _, v := range views {
		if err := updateRescans(st, v.ViewID, func(r []string) []string { return store.MergeRescans(r, []string{""}) }); err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 1
		}
	}
	cfg.Views.ExcludedNames = next
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	for _, r := range rules {
		fmt.Fprintf(stdout, "no longer excluded: name %s\n", r.Pattern)
	}
	fmt.Fprintln(stdout, "the daemon walks every view again on its next cycle to fetch what the rule hid; on a large NAS that takes a while")
	return 0
}

// excludeDefaults adds the default name rules and the home view's default paths that are missing.
func excludeDefaults(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("exclude defaults", flag.ContinueOnError)
	viewName := fs.String("view", "", "the view that gets the default excluded paths (default: the home view)")
	if code, ok := parseFlags(fs, excludeHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(excludeHelp, fs.Args(), stderr); !ok {
		return code
	}
	st, cfg, code := openConfigForCommand(stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = st.Close() }()
	var missing []config.ExcludedName
	for _, d := range config.DefaultExcludedNames() {
		if !slices.ContainsFunc(cfg.Views.ExcludedNames, func(n config.ExcludedName) bool { return sameRule(n, d) }) {
			missing = append(missing, d)
		}
	}
	if len(missing) > 0 {
		if rc := addNameRules(st, cfg, missing, stdout, stderr); rc != 0 {
			return rc
		}
		var err error
		if cfg, err = loadConfigFromStore(st); err != nil {
			fmt.Fprintf(stderr, "synosync: reload config: %s\n", userErr(err))
			return 1
		}
	}
	want := *viewName
	if want == "" {
		want = "home"
	}
	viewID, name, err := resolveExcludeView(st, cfg, want)
	if err != nil {
		fmt.Fprintf(stdout, "default paths (%s) not excluded: %s\n", strings.Join(config.DefaultExcludedPaths, ", "), userErr(err))
		return 0
	}
	var paths []string
	for _, p := range config.DefaultExcludedPaths {
		if !cfg.Views.IsExcluded(viewID, p) {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		if len(missing) == 0 {
			fmt.Fprintln(stdout, "all defaults are already in place")
		}
		return 0
	}
	return excludeAdd(st, cfg, viewID, name, paths, stdout, stderr)
}

// sameRule reports whether two name rules are the same rule (patterns compare case-insensitively,
// as they match).
func sameRule(a, b config.ExcludedName) bool {
	return strings.EqualFold(a.Pattern, b.Pattern) && a.FilesOnly == b.FilesOnly
}

// describeRule renders a name rule for a person.
func describeRule(r config.ExcludedName) string {
	s := fmt.Sprintf("%q", r.Pattern)
	if r.FilesOnly {
		s += " (files only)"
	}
	return s
}

// printExclusions writes the excluded paths, then the name rules.
func printExclusions(w io.Writer, cfg config.Config) {
	if len(cfg.Views.Excluded) == 0 && len(cfg.Views.ExcludedNames) == 0 {
		fmt.Fprintln(w, "Nothing is excluded (the NAS's own names always are).")
		return
	}
	for _, ex := range cfg.Views.Excluded {
		name := ex.ViewName
		if name == "" {
			name = fmt.Sprintf("view %d", ex.ViewID)
		}
		fmt.Fprintf(w, "%s:/%s\n", name, ex.Path)
	}
	for _, r := range cfg.Views.ExcludedNames {
		fmt.Fprintf(w, "name %s\n", describeRule(r))
	}
}

// homeView is the view the default excluded paths belong to: the one called "home" (My Drive),
// from the selection or, when every view syncs, from the server's list. nil when there is none
// or the list cannot be read - the paths are then simply not added.
func homeView(ctx context.Context, lister viewLister, views config.Views) *config.SelectedView {
	for _, sv := range views.Selected {
		if strings.EqualFold(sv.Name, "home") {
			return &sv
		}
	}
	if len(views.Selected) > 0 {
		return nil
	}
	available, err := lister.QueryUserInfo(ctx)
	if err != nil {
		return nil
	}
	for _, v := range available {
		if strings.EqualFold(v.Name, "home") && v.ViewID > 0 {
			return &config.SelectedView{ID: uint64(v.ViewID), Name: normpath.Normalize(v.Name)} // #nosec G115 -- checked positive
		}
	}
	return nil
}

// applyDefaultExclusions adds the default name rules and, given a home view, its default paths.
func applyDefaultExclusions(v *config.Views, home *config.SelectedView) {
	for _, d := range config.DefaultExcludedNames() {
		if !slices.ContainsFunc(v.ExcludedNames, func(n config.ExcludedName) bool { return sameRule(n, d) }) {
			v.ExcludedNames = append(v.ExcludedNames, d)
		}
	}
	if home == nil {
		return
	}
	for _, p := range config.DefaultExcludedPaths {
		if !v.IsExcluded(home.ID, p) {
			v.Excluded = append(v.Excluded, config.ExcludedPath{ViewID: home.ID, ViewName: home.Name, Path: p})
		}
	}
}

// describeDefaultExclusions tells a person pairing what is left out and how to change it.
func describeDefaultExclusions(w io.Writer, v config.Views, home *config.SelectedView) {
	if home != nil {
		fmt.Fprintf(w, "not synced by default, as in the official client: %s:/%s\n",
			home.Name, strings.Join(config.DefaultExcludedPaths, ", "+home.Name+":/"))
	}
	fmt.Fprintf(w, "and %d name rule(s) such as thumbs.db and *.tmp; see `synosync exclude list`, change with `synosync exclude remove`\n",
		len(v.ExcludedNames))
}
