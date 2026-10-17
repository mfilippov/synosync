package engine

// The in-package two-engine harness.
//
// docs/DESIGN §12 states that the never-lose invariants "are only really exercised by
// scenarios with TWO live engines: a single-client harness has no second replica that could
// diverge, so the shapes are structurally unreachable for it". Until now the only such
// scenarios lived in test/integration/twoclient*_test.go, which is build-tagged, env-gated on
// a live DSM and excluded from `make test` and from CI - so the invariants the whole design
// is organised around were pinned exclusively by tests that never run.
//
// This harness closes that: two engines, two stores, two sync roots, one shared fakeDSM. Each
// engine commits under its own session id, so each sees the other's writes as remote events
// and its own as self-echo, which is the property the scenarios need.
//
// It drives the engine through its real cycle (cold start, scan, drain, poll, drain) rather
// than calling applies directly, because several of the shapes worth testing are ORDERING
// shapes: the drain runs before the poll, so a queued op is applied against a tree the poll
// has not yet described.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// dsmEngine is one client: an engine, its own store and its own sync root, all bound to one
// session id over the shared server.
type dsmEngine struct {
	e       *Engine
	st      *store.Store
	dsm     *fakeDSM
	root    string
	session string
}

// newDSMEngine builds a client over d. When coldStart is false the views are marked already
// initialised at the server's current cursor, which is the ordinary incremental state; pass
// true to make the engine run its full-catalog pull on the first cycle.
func newDSMEngine(t *testing.T, d *fakeDSM, session string, coldStart bool) *dsmEngine {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	for _, v := range d.userInfo() {
		rec := store.View{
			Name:            v.Name,
			NodeID:          v.NodeID,
			RootNodeID:      v.RootNodeID,
			Privilege:       v.Privilege,
			InitialSyncDone: !coldStart,
		}
		if !coldStart {
			// Already-initialised means "everything the log holds so far is accounted for";
			// leaving the cursor at zero would replay the seeded history as fresh events.
			rec.LastSeenSyncID = d.maxSyncID(v.ViewID)
		}
		if err := st.PutView(uint64(v.ViewID), rec); err != nil { // #nosec G115 -- test view ids are small and positive
			t.Fatalf("PutView %d: %v", v.ViewID, err)
		}
		if err := os.MkdirAll(filepath.Join(root, v.Name), 0o755); err != nil {
			t.Fatalf("seed view dir: %v", err)
		}
	}
	e := New(st, &dsmTransport{client: &dsmClient{dsm: d, session: session}}, root, session, discardLogger())
	return &dsmEngine{e: e, st: st, dsm: d, root: root, session: session}
}

// viewDir is the on-disk directory backing a view.
func (n *dsmEngine) viewDir(name string) string { return filepath.Join(n.root, name) }

// write puts content at a view-relative path, creating parents.
func (n *dsmEngine) write(t *testing.T, viewName, rel, content string) {
	t.Helper()
	p := filepath.Join(n.viewDir(viewName), filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

// read returns the on-disk content at a view-relative path, and whether it exists.
func (n *dsmEngine) read(t *testing.T, viewName, rel string) (string, bool) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(n.viewDir(viewName), filepath.FromSlash(rel)))
	if os.IsNotExist(err) {
		return "", false
	}
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(b), true
}

// names lists the entries directly under a view-relative directory, sorted by ReadDir.
func (n *dsmEngine) names(t *testing.T, viewName, rel string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(n.viewDir(viewName), filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("readdir %s: %v", rel, err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// settle runs n full cycles, clearing the scan throttle before each so a test does not have
// to wait out limits.local_scan_interval between them.
func (n *dsmEngine) settle(t *testing.T, cycles int) {
	t.Helper()
	for i := 0; i < cycles; i++ {
		n.e.lastScan = time.Time{}
		if err := n.e.cycle(context.Background()); err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
	}
}

// TestTwoEngineRoundTrip is the harness's own smoke test: a file created on one client
// reaches the other through the server, with no scripted replies anywhere in the path.
func TestTwoEngineRoundTrip(t *testing.T) {
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	a := newDSMEngine(t, d, "sess-a", false)
	b := newDSMEngine(t, d, "sess-b", false)

	a.write(t, "Photos", "note.txt", "hello from A")
	a.settle(t, 2)

	if got, ok := d.content(7, "note.txt"); !ok || string(got) != "hello from A" {
		t.Fatalf("server content = %q present=%v, want %q", got, ok, "hello from A")
	}

	b.settle(t, 2)
	if got, ok := b.read(t, "Photos", "note.txt"); !ok || got != "hello from A" {
		t.Fatalf("B's disk = %q present=%v, want %q", got, ok, "hello from A")
	}
}

// TestTwoEngineSelfEchoNotReapplied pins the property the harness depends on: a client's own
// write comes back in the log attributed to its own session and must not be re-applied as a
// remote change.
func TestTwoEngineSelfEchoNotReapplied(t *testing.T) {
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	a := newDSMEngine(t, d, "sess-a", false)

	a.write(t, "Photos", "note.txt", "written once")
	a.settle(t, 3)

	if got, ok := a.read(t, "Photos", "note.txt"); !ok || got != "written once" {
		t.Fatalf("A's disk = %q present=%v, want the original bytes", got, ok)
	}
	// A single node, no _Conflict twin: the echo of A's own upload was recognised.
	if names := a.names(t, "Photos", "."); len(names) != 1 {
		t.Errorf("A's view holds %v, want exactly note.txt", names)
	}
	if paths := d.paths(7); len(paths) != 1 || paths[0] != "note.txt" {
		t.Errorf("server holds %v, want exactly [note.txt]", paths)
	}
}
