package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
)

// fakeLister replays a canned view enumeration, so the selection flow is driven
// without a server.
type fakeLister struct {
	views []actions.View
	err   error
}

func (f fakeLister) QueryUserInfo(context.Context) ([]actions.View, error) {
	return f.views, f.err
}

// listerOf builds an enumeration from names, numbering the ids from 1.
func listerOf(names ...string) fakeLister {
	out := make([]actions.View, 0, len(names))
	for i, n := range names {
		out = append(out, actions.View{ViewID: int64(i + 1), Name: n})
	}
	return fakeLister{views: out}
}

// choose runs the selection with no prior selection stored.
func choose(t *testing.T, l viewLister, requested string, p prompter) (config.Views, string, error) {
	t.Helper()
	var out bytes.Buffer
	v, _, err := selectViews(context.Background(), l, requested, config.Views{}, false, p, &out)
	return v, out.String(), err
}

// TestSelectViewsSingleViewGoesFlat is the case the feature exists for: one view
// is not a choice, so it is not offered as one, and its contents go straight into
// the sync root instead of a subdirectory nobody asked for.
func TestSelectViewsSingleViewGoesFlat(t *testing.T) {
	v, out, err := choose(t, listerOf("home"), "", &noPromptPrompter{t: t})
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if !v.Flat || len(v.Selected) != 1 || v.Selected[0].Name != "home" || v.Selected[0].ID != 1 {
		t.Fatalf("Views = %+v, want flat with just home", v)
	}
	if !strings.Contains(out, "syncing 1 view (home)") {
		t.Errorf("stdout = %q, want the layout described", out)
	}
}

// TestSelectViewsKeepsAStoredSelection is the fix for the worst hole in the first
// version. A re-pair - to a new server, or to re-capture a rotated pin - must not
// re-decide where an existing tree lives. Previously a one-view account
// short-circuited the prompt and flipped an established per-view install to flat,
// after which the engine reported the whole tree as locally deleted, to the
// server.
func TestSelectViewsKeepsAStoredSelection(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 1, Name: "home"}}}
	var out bytes.Buffer
	got, _, err := selectViews(context.Background(), listerOf("home"), "", prior, true, &noPromptPrompter{t: t}, &out)
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if got.Flat {
		t.Fatal("a re-pair flipped an established per-view install to flat")
	}
	if got.LayoutIdentity() != prior.LayoutIdentity() {
		t.Fatalf("layout identity changed on a re-pair: %q -> %q", prior.LayoutIdentity(), got.LayoutIdentity())
	}
	if !strings.Contains(out.String(), "keeping the stored layout") {
		t.Errorf("stdout = %q, want it to say the stored selection was kept", out.String())
	}
}

// TestSelectViewsExplicitRequestOverridesAStoredSelection: keeping the stored
// layout is the default, not a lock - an explicit --views is still a request.
// Whether it is ALLOWED is guardViewChange's decision, not this one's.
func TestSelectViewsExplicitRequestOverridesAStoredSelection(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 1, Name: "home"}}}
	var out bytes.Buffer
	got, _, err := selectViews(context.Background(), listerOf("home", "Team"), "Team", prior, true, &noPromptPrompter{t: t}, &out)
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if len(got.Selected) != 1 || got.Selected[0].Name != "Team" {
		t.Fatalf("Views = %+v, want Team", got)
	}
}

// TestSelectViewsAcceptsAll covers the spelling README and `pair --help` both
// document. It used to be routed to the name matcher and rejected after
// authentication, so a provisioning script copied from the docs failed at the
// pairing step.
func TestSelectViewsAcceptsAll(t *testing.T) {
	for _, spelling := range []string{"all", "ALL", " all "} {
		t.Run(spelling, func(t *testing.T) {
			v, _, err := choose(t, listerOf("home", "Team", "Photos"), spelling, &noPromptPrompter{t: t})
			if err != nil {
				t.Fatalf("selectViews(%q): %v", spelling, err)
			}
			if v.Flat {
				t.Error("all views selected but the layout is flat")
			}
			if len(v.Selected) != 0 {
				t.Errorf("Selected = %v, want empty (meaning every view)", v.Selected)
			}
		})
	}
}

// TestSelectViewsPromptDefaultsToAll pins the answer that cannot surprise
// anybody: pressing Enter keeps every view.
func TestSelectViewsPromptDefaultsToAll(t *testing.T) {
	v, out, err := choose(t, listerOf("home", "Team", "Photos"), "", &fakePrompter{})
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if v.Flat || len(v.Selected) != 0 {
		t.Errorf("Views = %+v, want the default", v)
	}
	for _, want := range []string{"The account can sync 3 views:", "1. Photos", "3. home"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %q", out, want)
		}
	}
}

// TestSelectViewsPromptAcceptsNumbers covers the numbers the prompt just printed.
func TestSelectViewsPromptAcceptsNumbers(t *testing.T) {
	// Listed sorted by name: Photos(3), Team(2), home(1).
	v, _, err := choose(t, listerOf("home", "Team", "Photos"), "", &fakePrompter{otps: []string{"1,3"}})
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if len(v.Selected) != 2 || v.Selected[0].Name != "Photos" || v.Selected[1].Name != "home" {
		t.Fatalf("Selected = %+v, want Photos and home", v.Selected)
	}
	if v.Flat {
		t.Error("two views selected but the layout is flat")
	}
}

func TestSelectViewsRequestedByNameAndNumber(t *testing.T) {
	for _, requested := range []string{"home", "3", " home "} {
		t.Run(requested, func(t *testing.T) {
			v, _, err := choose(t, listerOf("home", "Team", "Photos"), requested, &noPromptPrompter{t: t})
			if err != nil {
				t.Fatalf("selectViews(%q): %v", requested, err)
			}
			if !v.Flat || len(v.Selected) != 1 || v.Selected[0].Name != "home" {
				t.Fatalf("Views = %+v, want flat with just home", v)
			}
		})
	}
}

// TestSelectViewsMatchesANonNFCName covers the normalisation the engine already
// applies to the same field. A folder created on macOS arrives decomposed; the
// engine stores it NFC, so a selection recorded verbatim would never match again
// and the daemon would sync nothing, silently.
func TestSelectViewsMatchesANonNFCName(t *testing.T) {
	const nfd = "Café" // Cafe + combining acute
	const nfc = "Café"
	v, _, err := choose(t, listerOf(nfd, "Team"), nfc, &noPromptPrompter{t: t})
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if len(v.Selected) != 1 || v.Selected[0].Name != nfc {
		t.Fatalf("Selected = %+v, want the NFC form", v.Selected)
	}
}

func TestSelectViewsRejectsBadRequests(t *testing.T) {
	cases := map[string]string{
		"unknown name":  "nosuch",
		"out of range":  "9",
		"zero":          "0",
		"duplicate":     "home,home",
		"names nothing": ",",
	}
	for name, requested := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := choose(t, listerOf("home", "Team"), requested, &noPromptPrompter{t: t}); err == nil {
				t.Errorf("selectViews(%q) = nil error, want a refusal", requested)
			}
		})
	}
}

// TestSelectViewsEmptyEnumerationKeepsTheStoredSelection: an enumeration that
// came back empty is not a reason to rewrite an existing choice.
func TestSelectViewsEmptyEnumerationKeepsTheStoredSelection(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 1, Name: "home"}}, Flat: true}
	var out bytes.Buffer
	got, _, err := selectViews(context.Background(), listerOf(), "", prior, true, &noPromptPrompter{t: t}, &out)
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if got.LayoutIdentity() != prior.LayoutIdentity() {
		t.Errorf("layout changed on an empty enumeration: %+v", got)
	}
}

func TestSelectViewsReportsAFailedEnumeration(t *testing.T) {
	l := fakeLister{err: errors.New("connection reset")}
	if _, _, err := choose(t, l, "", &noPromptPrompter{t: t}); err == nil {
		t.Fatal("selectViews = nil error, want the enumeration failure")
	}
}

// TestUsableViewsDropsUnusableEntries: an unnamed view has no name to select by,
// a duplicate name cannot be addressed unambiguously, and a non-positive id is a
// malformed enumeration.
func TestUsableViewsDropsUnusableEntries(t *testing.T) {
	got := usableViews([]actions.View{
		{ViewID: 1, Name: "home"},
		{ViewID: 2, Name: ""},
		{ViewID: 3, Name: "home"},
		{ViewID: 0, Name: "zero"},
		{ViewID: -1, Name: "negative"},
		{ViewID: 4, Name: "Team"},
	})
	if len(got) != 2 || got[0].Name != "Team" || got[1].Name != "home" {
		t.Fatalf("usableViews = %+v, want Team and home", got)
	}
	if got[1].ID != 1 {
		t.Errorf("home resolved to id %d, want 1 (the first occurrence)", got[1].ID)
	}
}

// TestSelectViewsKeepsAZeroValueLayoutOnAnEstablishedInstall is the pre-feature
// upgrade path. An install paired by a binary that predates config.Views carries
// the ZERO value while its tree sits per-view on disk. Recognising a stored
// selection by len(Selected) > 0 read that as "unset", so a one-view account fell
// through to the flat branch, the gate saw a relocation, and re-pairing became
// impossible - with `synosync unlink`, which discards local state, as the only
// offered way out.
func TestSelectViewsKeepsAZeroValueLayoutOnAnEstablishedInstall(t *testing.T) {
	var out bytes.Buffer
	got, _, err := selectViews(context.Background(), listerOf("home"), "", config.Views{}, true, &noPromptPrompter{t: t}, &out)
	if err != nil {
		t.Fatalf("selectViews: %v", err)
	}
	if got.Flat {
		t.Fatal("a zero-value install was flipped to flat on re-pair")
	}
	if got.LayoutIdentity() != "per-view" {
		t.Fatalf("LayoutIdentity = %q, want per-view", got.LayoutIdentity())
	}
}

// TestDeriveLayoutIsSharedByBothWriters pins the derivation both `pair` and
// `config set` use. They used to order the same two rules differently and
// disagreed on a one-view account, so restating the current selection read as a
// relocation and was refused - making the key unusable on exactly the install the
// flat layout exists for.
func TestDeriveLayoutIsSharedByBothWriters(t *testing.T) {
	one := []config.SelectedView{{ID: 1, Name: "home"}}
	two := append(append([]config.SelectedView{}, one...), config.SelectedView{ID: 2, Name: "Team"})

	// The whole of a one-view account: flat, and NOT collapsed to the default,
	// which would throw away the layout just chosen.
	got := deriveLayout(one, one)
	if !got.Flat || len(got.Selected) != 1 {
		t.Errorf("deriveLayout(1 of 1) = %+v, want flat", got)
	}
	// The whole of a two-view account: the default, so a view shared later still
	// syncs.
	got = deriveLayout(two, two)
	if got.Flat || len(got.Selected) != 0 {
		t.Errorf("deriveLayout(2 of 2) = %+v, want the default", got)
	}
	// One of two: flat, explicitly selected.
	got = deriveLayout(one, two)
	if !got.Flat || len(got.Selected) != 1 {
		t.Errorf("deriveLayout(1 of 2) = %+v, want flat", got)
	}
}

// TestResolveViewTokenPrefersANameOverAPosition: trying the number first made a
// view literally called "3" unselectable - the token resolved to whatever
// happened to be third in the list.
func TestResolveViewTokenPrefersANameOverAPosition(t *testing.T) {
	views := []config.SelectedView{
		{ID: 10, Name: "1"},
		{ID: 20, Name: "Team"},
		{ID: 30, Name: "home"},
	}
	got, err := resolveViewToken("1", views)
	if err != nil {
		t.Fatalf("resolveViewToken: %v", err)
	}
	if got.ID != 10 {
		t.Errorf("resolved to view %d, want the one NAMED \"1\" (10)", got.ID)
	}
	// A number that is not a name still works as a position.
	got, err = resolveViewToken("2", views)
	if err != nil {
		t.Fatalf("resolveViewToken: %v", err)
	}
	if got.ID != 20 {
		t.Errorf("resolved to view %d, want the second entry (20)", got.ID)
	}
}

// TestRebindSelectionAfterAServerMigration covers the documented
// `pair --host nasB` flow. View ids are server-scoped, so a stored selection
// matches nothing on a different server: every view suppresses, and the daemon
// connects and syncs nothing with only an Info line. A flat install could not
// even repair it - re-pointing the selection changes the layout identity, which
// the gate refuses.
func TestRebindSelectionAfterAServerMigration(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}}, Flat: true}
	// The new server numbers the same folder differently.
	newServer := []config.SelectedView{{ID: 42, Name: "home"}, {ID: 43, Name: "Team"}}

	got, rebound := rebindSelection(prior, newServer)
	if !rebound {
		t.Fatal("a selection whose ids match nothing was not rebound by name")
	}
	if len(got.Selected) != 1 || got.Selected[0].ID != 42 {
		t.Fatalf("Selected = %+v, want the new id for home", got.Selected)
	}
	if !got.Flat {
		t.Error("the layout was lost in the rebind")
	}
}

// TestRebindSelectionLeavesALiveSelectionAlone: one surviving id means this is
// the same server, so nothing is guessed.
func TestRebindSelectionLeavesALiveSelectionAlone(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}, {ID: 8, Name: "Team"}}}
	same := []config.SelectedView{{ID: 7, Name: "home"}, {ID: 99, Name: "Team"}}
	if _, rebound := rebindSelection(prior, same); rebound {
		t.Error("rebound a selection that still has a live id")
	}
}

// TestRebindSelectionRefusesAPartialMatch: an account whose shares changed is
// not a migration, and guessing there could re-point the sync root at somebody
// else's folder.
func TestRebindSelectionRefusesAPartialMatch(t *testing.T) {
	prior := config.Views{Selected: []config.SelectedView{{ID: 7, Name: "home"}, {ID: 8, Name: "Team"}}}
	partial := []config.SelectedView{{ID: 42, Name: "home"}}
	if _, rebound := rebindSelection(prior, partial); rebound {
		t.Error("rebound a selection only half of which matched by name")
	}
}
