package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
)

// TestNotRunningHintCarriesTheStopReason pins the whole point of persisting it.
//
// Under a service manager the daemon's own stderr hint reaches nobody: the supervisor restarts
// it, it fails the same way and dies again, and every control verb reports only "daemon is not
// running". The reason and its remedy have to survive the process.
func TestNotRunningHintCarriesTheStopReason(t *testing.T) {
	dir := t.TempDir()
	paths := config.Paths{StateDir: dir}

	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	// A recorded identity: the hint asks whether the install is PAIRED, not merely whether a
	// state database exists.
	cfg := config.Default()
	cfg.DeviceUUID = "0123456789abcdef0123456789abcdef"
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.PutLastStop(store.LastStop{
		Reason: "the session is no longer valid",
		Hint:   "synosync login",
		At:     1_700_000_000,
	}); err != nil {
		t.Fatalf("PutLastStop: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	prev := openConfigStore
	openConfigStore = func() (*store.Store, error) { return store.Open(paths.StateDB()) }
	t.Cleanup(func() { openConfigStore = prev })

	hint := notRunningHint(paths)
	for _, want := range []string{
		"daemon is not running",
		"the session is no longer valid",
		"synosync login",
	} {
		if !strings.Contains(hint, want) {
			t.Errorf("hint missing %q:\n%s", want, hint)
		}
	}
}

// TestNotRunningHintWithoutAReasonIsUnchanged pins that a daemon which was simply never started
// still gets the plain hint, with no empty "it last stopped because" line.
func TestNotRunningHintWithoutAReasonIsUnchanged(t *testing.T) {
	dir := t.TempDir()
	paths := config.Paths{StateDir: dir}
	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	cfg := config.Default()
	cfg.DeviceUUID = "0123456789abcdef0123456789abcdef"
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	prev := openConfigStore
	openConfigStore = func() (*store.Store, error) { return store.Open(paths.StateDB()) }
	t.Cleanup(func() { openConfigStore = prev })

	if hint := notRunningHint(paths); hint != "synosync: daemon is not running (start it with `synosync daemon`)" {
		t.Errorf("hint = %q, want the plain one", hint)
	}
}

// TestNotRunningHintOnAFreshInstallDoesNotOpenAStore pins that the not-paired branch still wins
// and that explaining a problem never creates a state database as a side effect.
func TestNotRunningHintOnAFreshInstallDoesNotOpenAStore(t *testing.T) {
	dir := t.TempDir()
	paths := config.Paths{StateDir: dir}

	prev := openConfigStore
	openConfigStore = func() (*store.Store, error) {
		t.Fatal("the fresh-install branch opened the state database")
		return nil, nil
	}
	t.Cleanup(func() { openConfigStore = prev })

	if hint := notRunningHint(paths); !strings.Contains(hint, "not paired yet") {
		t.Errorf("hint = %q, want the not-paired advice", hint)
	}
	if _, err := filepath.Glob(filepath.Join(dir, "*")); err != nil {
		t.Fatalf("glob: %v", err)
	}
}

// TestLastStopRoundTripsAndClears pins the store side: recorded, read back, and gone once the
// daemon comes up again.
func TestLastStopRoundTripsAndClears(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	if _, ok, err := st.GetLastStop(); err != nil || ok {
		t.Fatalf("a fresh store reports a stop reason: ok=%v err=%v", ok, err)
	}
	want := store.LastStop{Reason: "the server certificate changed", Hint: "synosync pair", At: 42}
	if err := st.PutLastStop(want); err != nil {
		t.Fatalf("PutLastStop: %v", err)
	}
	got, ok, err := st.GetLastStop()
	if err != nil || !ok || got != want {
		t.Fatalf("GetLastStop = %+v ok=%v err=%v, want %+v", got, ok, err, want)
	}
	if err := st.ClearLastStop(); err != nil {
		t.Fatalf("ClearLastStop: %v", err)
	}
	if _, ok, err := st.GetLastStop(); err != nil || ok {
		t.Fatalf("the reason survived the clear: ok=%v err=%v", ok, err)
	}
}
