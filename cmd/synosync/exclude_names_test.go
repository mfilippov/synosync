package main

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

func okAuth(*testing.T) replyFunc {
	return func(uint8, *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(auth.CodeOK)).Set("session", sproto.String("abcdef0123456789abcdef0123456789"))
	}
}

// TestFirstPairStartsWithTheDefaultRules: a first pairing gets the official client's name rules.
// (The mock server lists no views, so there is no home view to exclude paths in.)
func TestFirstPairStartsWithTheDefaultRules(t *testing.T) {
	srv := newPairMockServer(t, okAuth(t))
	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	writeServerConfig(t, path, srv, "alice", nil)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, path)
	if !slices.Equal(cfg.Views.ExcludedNames, config.DefaultExcludedNames()) {
		t.Errorf("ExcludedNames = %+v, want the defaults", cfg.Views.ExcludedNames)
	}
}

// TestRepairKeepsTheUsersRules: a re-pair leaves the rules as the user set them, so one they
// removed does not come back.
func TestRepairKeepsTheUsersRules(t *testing.T) {
	srv := newPairMockServer(t, okAuth(t))
	path := setupCLI(t, &fakePrompter{password: "s3cret"})
	mine := []config.ExcludedName{{Pattern: "*.bak"}}
	writeServerConfig(t, path, srv, "alice", func(c *config.Config) {
		c.DeviceUUID, c.Server.DsID = "0123456789abcdef0123456789abcdef", testDsID
		c.Views.ExcludedNames = mine
	})
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pair"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if got := loadStoredConfig(t, path).Views.ExcludedNames; !slices.Equal(got, mine) {
		t.Errorf("ExcludedNames = %+v after a re-pair, want the user's %+v", got, mine)
	}
}

// TestApplyDefaultExclusionsExcludesPhotosAndBackupInHome.
func TestApplyDefaultExclusionsExcludesPhotosAndBackupInHome(t *testing.T) {
	var v config.Views
	applyDefaultExclusions(&v, &config.SelectedView{ID: 3, Name: "home"})
	for _, p := range config.DefaultExcludedPaths {
		if !v.IsExcluded(3, p) {
			t.Errorf("%s not excluded in the home view", p)
		}
	}
	if v.IsExcluded(4, "Photos") {
		t.Error("Photos excluded in another view")
	}
	if err := v.Validate(); err != nil {
		t.Errorf("defaults do not validate: %v", err)
	}
}

// TestExcludeNameRules drives add and remove of name rules over a synced view.
func TestExcludeNameRules(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	st, err := store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.PutView(1, store.View{Name: "home", NodeID: "n", InitialSyncDone: true}); err != nil {
		t.Fatal(err)
	}
	for p, typ := range map[string]string{"a/thumbs.db": "file", "x/tmp": "dir", "x/tmp/keep.txt": "file", "y/b.tmp": "file"} {
		if err := st.PutBaseline(1, p, store.Baseline{FileID: "id-" + p, FileType: typ, State: store.StateSynced}); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.Close()

	var stdout, stderr bytes.Buffer
	if code := run([]string{"exclude", "add", "--name", "--files", "*.tmp", "tmp"}, &stdout, &stderr); code != 0 {
		t.Fatalf("add --name: exit %d (stderr %q)", code, stderr.String())
	}
	if code := run([]string{"exclude", "add", "--name", "THUMBS.DB"}, &stdout, &stderr); code != 0 {
		t.Fatalf("add --name: exit %d (stderr %q)", code, stderr.String())
	}
	st, err = store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	for p, wantGone := range map[string]bool{"a/thumbs.db": true, "y/b.tmp": true, "x/tmp": false, "x/tmp/keep.txt": false} {
		_, err := st.GetBaseline(1, p)
		if gone := errors.Is(err, store.ErrNotFound); gone != wantGone {
			t.Errorf("baseline %s gone=%v, want %v (a files-only rule must not hide the folder tmp)", p, gone, wantGone)
		}
	}
	_ = st.Close()

	stdout.Reset()
	if code := run([]string{"exclude", "remove", "--name", "*.tmp"}, &stdout, &stderr); code != 0 {
		t.Fatalf("remove --name: exit %d (stderr %q)", code, stderr.String())
	}
	if code := run([]string{"exclude", "remove", "--name", "@eaDir"}, &stdout, &stderr); code == 0 {
		t.Error("removing a NAS system name succeeded, want refused")
	}
	st, err = store.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	v, err := st.GetView(1)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(v.Rescan, []string{""}) {
		t.Errorf("Rescan = %q, want the whole view after lifting a name rule", v.Rescan)
	}
}

// TestExcludeDefaultsAddsWhatIsMissing: an install that predates the defaults gets them on request.
func TestExcludeDefaultsAddsWhatIsMissing(t *testing.T) {
	statePath := setupCLI(t, &fakePrompter{})
	seedViews(t, statePath, "home")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"exclude", "defaults"}, &stdout, &stderr); code != 0 {
		t.Fatalf("defaults: exit %d (stderr %q)", code, stderr.String())
	}
	cfg := loadStoredConfig(t, statePath)
	if len(cfg.Views.ExcludedNames) != len(config.DefaultExcludedNames()) {
		t.Errorf("ExcludedNames = %d rule(s), want %d", len(cfg.Views.ExcludedNames), len(config.DefaultExcludedNames()))
	}
	if !cfg.Views.IsExcluded(1, "Photos") || !cfg.Views.IsExcluded(1, "Backup") {
		t.Errorf("Excluded = %+v, want Photos and Backup in home", cfg.Views.Excluded)
	}
	stdout.Reset()
	if code := run([]string{"exclude", "defaults"}, &stdout, &stderr); code != 0 || !bytes.Contains(stdout.Bytes(), []byte("already in place")) {
		t.Errorf("second defaults = %q (exit %d), want a no-op", stdout.String(), code)
	}
}
