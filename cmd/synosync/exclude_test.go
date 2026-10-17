package main

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
)

// seedSyncedView records one view that has finished its first sync, with a tracked file inside
// and outside Photos and a queued download under it.
func seedSyncedView(t *testing.T, statePath string) {
	t.Helper()
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.PutView(1, store.View{Name: "home", NodeID: "n", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for _, p := range []string{"Photos", "Photos/a.jpg", "Docs/b.txt"} {
		if err := st.PutBaseline(1, p, store.Baseline{FileID: "id-" + p, State: store.StateSynced}); err != nil {
			t.Fatalf("PutBaseline: %v", err)
		}
	}
	if _, err := st.Enqueue(store.Op{Type: store.OpDownload, Direction: store.DirInbound, ViewID: 1, Path: "Photos/c.jpg"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

// TestExcludeAddForgetsTheSubtreeAndRemoveRescansIt drives both halves end to end.
func TestExcludeAddForgetsTheSubtreeAndRemoveRescansIt(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedSyncedView(t, statePath)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"exclude", "add", "/Photos/"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exclude add: exit %d (stderr: %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if len(cfg.Views.Excluded) != 1 || cfg.Views.Excluded[0].Path != "Photos" || cfg.Views.Excluded[0].ViewID != 1 {
		t.Fatalf("Excluded = %+v, want Photos in view 1", cfg.Views.Excluded)
	}
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	for _, p := range []string{"Photos", "Photos/a.jpg"} {
		if _, err := st.GetBaseline(1, p); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("baseline %s: err = %v, want forgotten", p, err)
		}
	}
	if _, err := st.GetBaseline(1, "Docs/b.txt"); err != nil {
		t.Errorf("baseline outside the exclusion: %v, want kept", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue holds %d op(s), want the download under Photos cancelled", n)
	}
	_ = st.Close()

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"exclude", "remove", "Photos"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exclude remove: exit %d (stderr: %q)", code, stderr.String())
	}
	if cfg := loadStoredConfig(t, statePath); len(cfg.Views.Excluded) != 0 {
		t.Errorf("Excluded = %+v after remove, want empty", cfg.Views.Excluded)
	}
	st, err = store.Open(statePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()
	v, err := st.GetView(1)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if !slices.Equal(v.Rescan, []string{"Photos"}) {
		t.Errorf("Rescan = %v, want the lifted path queued for a rescan", v.Rescan)
	}
}

// TestExcludeRefusesTheViewRootAndEscapes: neither names a subtree an exclusion can stand for.
func TestExcludeRefusesTheViewRootAndEscapes(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedSyncedView(t, statePath)
	for _, p := range []string{"/", "../x"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"exclude", "add", p}, &stdout, &stderr); code == 0 {
			t.Errorf("exclude add %q succeeded, want refused", p)
		}
	}
}

// TestChangingTheViewSelectionKeepsExclusions: `config set views.selected` rebuilds the view
// settings, and must carry the exclusions across.
func TestChangingTheViewSelectionKeepsExclusions(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedSyncedView(t, statePath)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"exclude", "add", "Photos"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exclude add: exit %d (stderr: %q)", code, stderr.String())
	}
	// Nothing is laid out flat yet only because nothing synced through the CLI; the guard that
	// refuses a relocation is about baselines, so drop the seeded ones first.
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = st.DropSubtree(1, func(string, bool) bool { return true })
	_ = st.Close()
	if code := run([]string{"config", "set", "views.selected", "home"}, &stdout, &stderr); code != 0 {
		t.Fatalf("config set: exit %d (stderr: %q)", code, stderr.String())
	}
	if cfg := loadStoredConfig(t, statePath); len(cfg.Views.Excluded) != 1 {
		t.Errorf("Excluded = %+v after changing the selection, want it kept", cfg.Views.Excluded)
	}
	stdout.Reset()
	if code := run([]string{"exclude", "list"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "home:/Photos") {
		t.Errorf("exclude list = %q (exit %d), want home:/Photos", stdout.String(), code)
	}
}

// TestStatusListsExclusionsAndRescans: both are shown under their view, so a folder that is
// not syncing on purpose does not look broken.
func TestStatusListsExclusionsAndRescans(t *testing.T) {
	var buf bytes.Buffer
	renderStatus(&buf, ipc.StatusReport{Views: []ipc.ViewStatus{{
		ID: 1, Name: "home", InitialSyncDone: true,
		Excluded: []string{"Photos"}, Rescanning: []string{"Movies"},
	}}})
	out := buf.String()
	for _, want := range []string{"Left out:  /Photos (synosync exclude list)", "Fetching:  /Movies in home"} {
		if !strings.Contains(out, want) {
			t.Errorf("status output lacks %q:\n%s", want, out)
		}
	}
}
