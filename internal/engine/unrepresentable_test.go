package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// unholdableName probes the host for a name its filesystem refuses to create, skipping where
// every candidate is a legitimate name (Unix-likes hold all of them).
func unholdableName(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{"a|b", "a:b", "a?b"} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			return name
		}
		_ = f.Close()
	}
	t.Skip("this host's filesystem holds every candidate name")
	return ""
}

// TestHostRepresentablePathMatchesTheFilesystem probes the host: a name with a character the
// local filesystem refuses (Win32 refuses <>:"|?*) must be unrepresentable, and one it creates
// must stay representable, so reconcile skips exactly the names that would otherwise queue and
// dead-letter at apply.
func TestHostRepresentablePathMatchesTheFilesystem(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a<b", "a>b", `a"b`, "a|b", "a?b", "a*b", "a\x01b", "plain"} {
		f, err := os.Create(filepath.Join(dir, name))
		created := err == nil
		if created {
			_ = f.Close()
		}
		if got := hostRepresentablePath(filepath.Join("sub", name)); got != created {
			t.Errorf("hostRepresentablePath(%q) = %v, but creating it on disk: err = %v", name, got, err)
		}
	}
}

func unrepresentableTraces(t *testing.T, st *store.Store) []store.DeadLetteredOp {
	t.Helper()
	all, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	var out []store.DeadLetteredOp
	for _, d := range all {
		if d.Type == store.OpUnrepresentableName {
			out = append(out, d)
		}
	}
	return out
}

// TestUnrepresentableServerNameIsTracedOnceAtItsFolder: a server folder this host cannot name
// is skipped with no op, and surfaces as ONE Dropped record naming the folder - not one per
// file under it, not again on the next event, and not again after a restart.
func TestUnrepresentableServerNameIsTracedOnceAtItsFolder(t *testing.T) {
	bad := unholdableName(t)
	e, st, _ := newManglingEngine(t)
	ops, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: bad + "/a.txt", Hash: "h", Size: 1},
		{SyncID: 2, Action: "create", FileID: "f2", Path: bad + "/b.txt", Hash: "h", Size: 1},
		{SyncID: 3, Action: "delete", FileID: "f3", Path: bad + "/c.txt"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("ops = %+v, want none for a folder this host cannot name", ops)
	}
	got := unrepresentableTraces(t, st)
	if len(got) != 1 || got[0].Path != bad || got[0].ViewID != 1 {
		t.Fatalf("traces = %+v, want one naming %q", got, bad)
	}

	// A restarted engine over the same store seeds its memo from the ring.
	e2 := New(st, e.transport, e.syncRoot, "sess", nil)
	if _, _, err := e2.reconcile(1, []actions.Event{
		{SyncID: 4, Action: "modify", FileID: "f1", Path: bad + "/a.txt", Hash: "h2", Size: 2},
	}, 3); err != nil {
		t.Fatalf("reconcile after restart: %v", err)
	}
	if n := len(unrepresentableTraces(t, st)); n != 1 {
		t.Errorf("traces after restart = %d, want still 1", n)
	}
}

// TestEscapingServerPathIsNotTracedAsAName: a ".."-escaping path is a containment failure, not a
// name the user can fix by renaming, so it is skipped without a Dropped record.
func TestEscapingServerPathIsNotTracedAsAName(t *testing.T) {
	e, st, _ := newManglingEngine(t)
	if _, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 1, Action: "create", FileID: "f1", Path: "../escape.txt", Hash: "h", Size: 1},
	}, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n := len(unrepresentableTraces(t, st)); n != 0 {
		t.Errorf("traces = %d, want none for an escaping path", n)
	}
}

// TestUnrepresentableTracesAreCappedWithASummary: past the per-view cap the rest are counted into
// one summary record, so a tree full of such names cannot retire the whole dead-letter ring.
func TestUnrepresentableTracesAreCappedWithASummary(t *testing.T) {
	bad := unholdableName(t)
	e, st, _ := newManglingEngine(t)
	var evs []actions.Event
	for i := range unrepresentableTraceCap + 10 {
		evs = append(evs, actions.Event{SyncID: int64(i + 1), Action: "create",
			FileID: fmt.Sprint("f", i), Path: fmt.Sprintf("%s%d.txt", bad, i), Hash: "h", Size: 1})
	}
	if _, _, err := e.reconcile(1, evs, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got := unrepresentableTraces(t, st)
	if len(got) != unrepresentableTraceCap+1 {
		t.Fatalf("traces = %d, want %d (the cap plus one summary)", len(got), unrepresentableTraceCap+1)
	}
	if last := got[len(got)-1]; last.Path != "" {
		t.Errorf("last trace = %+v, want the pathless summary", last)
	}
}

// TestColdStartTracesAnUnrepresentableServerName: the cold-start planner skips such a name with
// the same Dropped record, while its containable siblings plan normally.
func TestColdStartTracesAnUnrepresentableServerName(t *testing.T) {
	bad := unholdableName(t)
	e, st := newReconcileEngine(t, "self")
	ops, err := e.coldStartReconcile(1,
		catalogMap(serverFile(bad+".pdf", "s1", "h1", 3), serverFile("ok.txt", "s2", "h2", 4)),
		nil, nil, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "ok.txt" {
		t.Fatalf("ops = %+v, want only the download of ok.txt", ops)
	}
	if got := unrepresentableTraces(t, st); len(got) != 1 || got[0].Path != bad+".pdf" {
		t.Errorf("traces = %+v, want one naming %q", got, bad+".pdf")
	}
}
