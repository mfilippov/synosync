package main

// View selection for `pair`.
//
// The choice has to happen here: views are only knowable from an authenticated
// session, and `pair` is the last point before the daemon's first cycle writes a
// directory tree. Any later choice is a choice about files that already exist
// somewhere, which is a relocation rather than a setting - see viewlayout.go for
// the gate that says so.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/normpath"
)

// allViews is the spelling that means "every view", accepted by --views, by the
// prompt, and by `config set views.selected`. One word, one meaning, everywhere:
// it was previously honoured by two of the three, so a documented
// `--views all` was parsed as the name of a view and rejected.
const allViews = "all"

// viewLister is the one call this file needs from an authenticated session. A
// seam so the selection flow is driven in tests without a server.
type viewLister interface {
	QueryUserInfo(ctx context.Context) ([]actions.View, error)
}

// selectViews resolves which views to sync and the layout that follows.
//
// prior is the selection already stored, and it wins whenever the operator did
// not ask for something else: a re-pair - to a new server, or to re-capture a
// rotated pin - must not re-decide where an existing tree lives. Only an explicit
// --views is treated as a request to change, and guardViewChange then decides
// whether that change is allowed at all.
//
// It returns the chosen selection AND the baseline the gate must compare it
// against. The two differ in exactly one case: a selection rebound onto a new
// server's view ids is the SAME layout under new numbering, so comparing it
// against the stored ids would read a migration as a relocation and refuse it.
func selectViews(ctx context.Context, lister viewLister, requested string, prior config.Views, established bool, p prompter, stdout io.Writer) (chosen, baseline config.Views, err error) {
	available, err := lister.QueryUserInfo(ctx)
	if err != nil {
		return config.Views{}, config.Views{}, fmt.Errorf("list the account's views: %w", err)
	}
	views := usableViews(available)
	if len(views) == 0 {
		// Nothing to choose between. Keep whatever is stored: an enumeration that
		// came back empty is not a reason to rewrite an existing selection.
		return prior, prior, nil
	}

	switch {
	case requested != "":
		selected, err := matchRequested(requested, views)
		if err != nil {
			return config.Views{}, config.Views{}, err
		}
		return layoutFor(selected, views, stdout), prior, nil

	case established:
		// Something is already synced and no change was asked for: keep the stored
		// layout, whatever it is.
		//
		// "Whatever it is" includes the ZERO value. An install paired by a binary
		// that predates this field carries it, and recognising a stored selection
		// by len(Selected) > 0 treated that as "unset" - so a one-view account fell
		// through to the flat branch, the gate saw a relocation, and re-pairing
		// became impossible with `synosync unlink` as the only offered way out.
		kept, rebound := rebindSelection(prior, views)
		if rebound {
			// The stored ids match nothing this account reports, but the NAMES all
			// do. That is a server migration - `pair --host nasB`, which README
			// documents - where the same folders carry new server-scoped ids.
			// Without re-resolving them the selection matches no view, the daemon
			// suppresses every one of them and syncs nothing, silently; and a flat
			// install could not even repair it, because re-pointing the selection
			// changes the layout identity and is refused.
			fmt.Fprintf(stdout, "\nthe stored selection was recorded against a different server;\n")
			fmt.Fprintf(stdout, "matched it by name onto this one: %s\n", strings.Join(kept.Names(), ", "))
			// The rebound value is ALSO the baseline: the same layout under new
			// numbering is not a relocation, and comparing it against the dead ids
			// would refuse the migration this exists to support.
			return kept, kept, nil
		}
		fmt.Fprintf(stdout, "\nkeeping the stored layout: %s\n", describeIdentity(prior))
		fmt.Fprintf(stdout, "change it with `synosync config set views.selected <names>`\n")
		return kept, prior, nil

	case len(views) == 1:
		// One view is not a choice, and asking "which of these one?" is noise.
		return layoutFor(views, views, stdout), prior, nil

	default:
		selected, err := promptForViews(views, p, stdout)
		if err != nil {
			return config.Views{}, config.Views{}, err
		}
		return layoutFor(selected, views, stdout), prior, nil
	}
}

// layoutFor turns a chosen set into the recorded selection, and describes it.
func layoutFor(selected, available []config.SelectedView, stdout io.Writer) config.Views {
	v := deriveLayout(selected, available)
	describeLayout(stdout, v, selected)
	return v
}

// deriveLayout turns a chosen set into the recorded selection.
//
// It is THE derivation: both writers of config.Views call it, because they used
// to order the same two rules differently and disagreed on a one-view account -
// `pair` made it flat, `config set` made it per-view. Restating the current
// selection was then read as a tree relocation and refused, which made the key
// unusable on exactly the install the flat layout exists for.
func deriveLayout(selected, available []config.SelectedView) config.Views {
	v := config.Views{Selected: selected, Flat: len(selected) == 1}
	// Selecting everything is recorded as the DEFAULT - an empty selection - so a
	// team folder shared later keeps syncing rather than being silently ignored.
	//
	// A single-view account is the exception, deliberately: there "everything" IS
	// the one view, and collapsing it would put that view in a subdirectory -
	// exactly what the flat layout exists to avoid. So `--views all`, `--views
	// home` and a plain `pair` all flatten a one-view account, which is the point:
	// the three spellings of the same choice must not produce three layouts.
	if len(selected) == len(available) && !v.Flat {
		v.Selected = nil
	}
	return v
}

// usableViews returns the account's views as selection entries: id plus the
// display name, sorted by name, with duplicates and unnamed views dropped.
//
// Names are NFC-normalised here, the same boundary the engine normalises at
// (refreshViews). It is display and matching text only - the id is what is ever
// compared - but an operator typing a name should match what they were shown.
func usableViews(views []actions.View) []config.SelectedView {
	seenID := make(map[uint64]bool, len(views))
	seenName := make(map[string]bool, len(views))
	out := make([]config.SelectedView, 0, len(views))
	for _, v := range views {
		if v.Name == "" || v.ViewID <= 0 {
			continue
		}
		id := uint64(v.ViewID)
		name := normpath.Normalize(v.Name)
		// A duplicate name cannot be addressed unambiguously by the operator, and
		// a repeated id is a malformed enumeration.
		if seenID[id] || seenName[name] {
			continue
		}
		seenID[id], seenName[name] = true, true
		out = append(out, config.SelectedView{ID: id, Name: name})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// matchRequested resolves a --views value (or a prompt answer) against what the
// account can see. Both names and 1-based list positions are accepted, because
// the prompt shows numbers and an operator scripting the same choice should be
// able to write down what they just read.
func matchRequested(requested string, views []config.SelectedView) ([]config.SelectedView, error) {
	if strings.EqualFold(strings.TrimSpace(requested), allViews) {
		return views, nil
	}
	var out []config.SelectedView
	seen := make(map[uint64]bool)
	for _, field := range strings.Split(requested, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		v, err := resolveViewToken(field, views)
		if err != nil {
			return nil, err
		}
		if seen[v.ID] {
			return nil, fmt.Errorf("view %q listed twice", v.Name)
		}
		seen[v.ID] = true
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, errors.New("no view was named")
	}
	return out, nil
}

// resolveViewToken maps one field onto a view.
func resolveViewToken(field string, views []config.SelectedView) (config.SelectedView, error) {
	// A NAME wins over a list position. Trying the number first made a view
	// literally called "3" unselectable: the token resolved to whatever happened
	// to be third. A name the operator can see beats an index they inferred.
	want := normpath.Normalize(field)
	for _, v := range views {
		if v.Name == want {
			return v, nil
		}
	}
	if n, err := strconv.Atoi(field); err == nil {
		if n < 1 || n > len(views) {
			return config.SelectedView{}, fmt.Errorf("view %d is out of range; the account has %d", n, len(views))
		}
		return views[n-1], nil
	}
	return config.SelectedView{}, fmt.Errorf("no view named %q; the account can sync: %s",
		field, strings.Join(viewNamesOf(views), ", "))
}

// viewNamesOf lists the display names, for an error message.
func viewNamesOf(views []config.SelectedView) []string {
	out := make([]string, 0, len(views))
	for _, v := range views {
		out = append(out, v.Name)
	}
	return out
}

// promptForViews asks which views to sync. Enter takes everything, which is both
// the historical behaviour and the answer that cannot surprise anybody.
func promptForViews(views []config.SelectedView, p prompter, stdout io.Writer) ([]config.SelectedView, error) {
	fmt.Fprintf(stdout, "\nThe account can sync %d views:\n", len(views))
	for i, v := range views {
		fmt.Fprintf(stdout, "  %d. %s\n", i+1, v.Name)
	}
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Enter the numbers to sync, comma-separated. One view is placed")
	fmt.Fprintln(stdout, "directly in the sync root; several get a subdirectory each.")

	answer, err := p.ReadLine("Sync which? [all]: ")
	if err != nil {
		return nil, fmt.Errorf("read the view selection: %w", err)
	}
	if answer = strings.TrimSpace(answer); answer == "" {
		return views, nil
	}
	return matchRequested(answer, views)
}

// describeLayout tells the operator what was decided, in terms of the
// directories they will see. The layout is the whole point of the choice, and it
// is not otherwise visible until the first sync creates - or does not create - a
// subdirectory.
func describeLayout(stdout io.Writer, v config.Views, selected []config.SelectedView) {
	switch {
	case v.Flat:
		fmt.Fprintf(stdout, "\nsyncing 1 view (%s), directly in the sync root\n", selected[0].Name)
	case len(v.Selected) == 0:
		fmt.Fprintf(stdout, "\nsyncing every view, one subdirectory each\n")
	default:
		fmt.Fprintf(stdout, "\nsyncing %d views, one subdirectory each:\n", len(selected))
		for _, sv := range selected {
			fmt.Fprintf(stdout, "  %s\n", sv.Name)
		}
	}
}

// rebindSelection re-resolves a stored selection's view ids against what this
// account reports, matching by name.
//
// View ids are server-scoped, so pairing an existing install to a DIFFERENT
// server leaves every stored id matching nothing: Views.Syncs then answers false
// for every view, all four engine gates suppress, and the daemon connects and
// syncs nothing with only an Info line to show for it. Matching the names back
// onto the new ids is what makes the documented `pair --host nasB` migration
// work.
//
// It rebinds only when NO stored id is present in the enumeration and EVERY
// stored name resolves to exactly one view. A partial match is left alone: that
// is an account whose shares changed, not a different server, and guessing there
// could silently re-point the sync root at somebody else's folder.
func rebindSelection(prior config.Views, views []config.SelectedView) (config.Views, bool) {
	if len(prior.Selected) == 0 {
		return prior, false
	}
	byID := make(map[uint64]bool, len(views))
	byName := make(map[string]config.SelectedView, len(views))
	for _, v := range views {
		byID[v.ID] = true
		byName[v.Name] = v
	}
	rebound := make([]config.SelectedView, 0, len(prior.Selected))
	for _, sv := range prior.Selected {
		if byID[sv.ID] {
			// At least one stored id is still live, so this is the same server.
			return prior, false
		}
		match, ok := byName[sv.Name]
		if !ok {
			return prior, false
		}
		rebound = append(rebound, match)
	}
	return config.Views{Selected: rebound, Flat: prior.Flat}, true
}
