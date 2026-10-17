package engine

// Tests for the host name-representability gate: a server path whose
// component the host would silently REWRITE rather than materialise verbatim - on Windows one
// ending in a trailing dot or space, which Win32 strips with no error at all - must never be
// applied, because every path-addressed gate downstream reads "the call succeeded" as "the path
// I addressed is the path I landed".
//
// The Windows arms are empirical where they can be: the host pins in
// TestWindowsSilentlyStripsTrailingDotOnDisk assert the mangling itself on the real filesystem,
// so the gate's premise is measured rather than assumed. The engine-level arms are seam-driven
// and run on every host with the per-GOOS verdict they expect.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestWindowsSilentlyStripsTrailingDotOnDisk pins the premise the whole gate rests on: on
// Windows, creating a name with a trailing dot or space SUCCEEDS and yields the stripped name,
// and a stat of the unstripped spelling aliases the stripped node. There is no errno to
// classify - which is why nametoolong_windows.go's old claim (ERROR_INVALID_NAME) was dead code
// and the class is closed lexically instead. On Unix the same names are ordinary and round-trip
// verbatim, which is why the gate is a no-op there.
func TestWindowsSilentlyStripsTrailingDotOnDisk(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"notes.", "notes "} {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile %q: %v", name, err)
		}
		stripped := filepath.Join(dir, strings.TrimRight(name, ". "))
		if runtime.GOOS == "windows" {
			if _, err := os.Lstat(stripped); err != nil {
				t.Errorf("Lstat(%q) after creating %q: %v; want the stripped name to exist",
					stripped, name, err)
			}
			// The unstripped spelling "exists" too - it aliases the stripped node, which is
			// exactly how the engine's path-addressed gates are deceived.
			if _, err := os.Lstat(p); err != nil {
				t.Errorf("Lstat(%q): %v; want the unstripped spelling to alias the stripped node", p, err)
			}
		} else if _, err := os.Lstat(stripped); !os.IsNotExist(err) {
			t.Errorf("Lstat(%q) on %s: err = %v; want IsNotExist (names are stored verbatim here)",
				stripped, runtime.GOOS, err)
		}
		_ = os.Remove(p)
		_ = os.Remove(stripped)
	}
}

// TestHostRepresentablePath is the per-OS truth table of the gate itself. Trailing dots and
// spaces are unrepresentable on Windows only; "." and ".." stay exempt (they are path syntax the
// lexical containment checks own, and reporting them unrepresentable would shadow those
// better-stated rejections); everything else is representable everywhere.
func TestHostRepresentablePath(t *testing.T) {
	windows := runtime.GOOS == "windows"
	cases := []struct {
		rel  string
		want bool
	}{
		{"notes.txt", true},
		{"sub/notes.txt", true},
		{".hidden", true},
		{"..hidden", true},
		{"a b/c d.txt", true},
		{".", true},
		{"..", true},
		{"a/..", true},
		{"notes.", !windows},
		{"notes ", !windows},
		{"sub./notes.txt", !windows},
		{"sub /notes.txt", !windows},
		{"a/b/deep.", !windows},
	}
	for _, c := range cases {
		rel := filepath.FromSlash(c.rel)
		if got := hostRepresentablePath(rel); got != c.want {
			t.Errorf("hostRepresentablePath(%q) on %s = %v, want %v", c.rel, runtime.GOOS, got, c.want)
		}
	}
}

// TestContainableRejectsUnrepresentableName is the reconcile-layer screen: containable() must
// refuse a mangled name on Windows for the same reason it already refuses a literal backslash
// there - two store keys would describe one on-disk file - and must keep accepting it on Unix,
// where such a name is legitimate and round-trips.
func TestContainableRejectsUnrepresentableName(t *testing.T) {
	windows := runtime.GOOS == "windows"
	for _, p := range []string{"notes.", "notes ", "sub./f.txt"} {
		if got := containable(p); got != !windows {
			t.Errorf("containable(%q) on %s = %v, want %v", p, runtime.GOOS, got, !windows)
		}
	}
	if !containable("notes.txt") {
		t.Error("containable(\"notes.txt\") = false, want true (an ordinary name)")
	}
}

// newManglingEngine builds a real-disk engine over a view directory, for the arms that must
// prove the DISK is untouched (not merely that no op was enqueued).
func newManglingEngine(t *testing.T) (*Engine, *store.Store, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(1, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	dl := &dlClient{
		content: map[string][]byte{"fdot": []byte("dot-content"), "ffoo": []byte("foo-content")},
		nodes: map[string]actions.Node{
			"fdot": {Hash: "hdot", Size: 11},
			"ffoo": {Hash: "hfoo", Size: 11},
		},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	return e, st, viewDir
}

// TestReconcileSkipsUnrepresentableCreate is the red test for a lone unrepresentable create: a
// peer on ext4 creates "notes." on the server. Before the gate the Windows client applied it -
// bytes landed under "notes", markSynced recorded a StateSynced baseline for the non-existent
// "notes.", the next scan read an untracked create plus a vanished node, and move detection pushed
// the server rename "notes." -> "notes", silently mutating the name for every replica. Now
// reconcile skips the event before any baseline is touched: no op, no baseline, nothing on disk,
// and the cursor still advances so the event is acknowledged rather than re-pulled forever. On Unix
// the name is legitimate and the download is enqueued as before.
func TestReconcileSkipsUnrepresentableCreate(t *testing.T) {
	e, st, viewDir := newManglingEngine(t)
	events := []actions.Event{
		{SyncID: 11, Action: "create", FileID: "fdot", Path: "notes.", Hash: "hdot", Size: 11, MTime: 100},
	}
	ops, cursor, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if cursor != 11 {
		t.Errorf("cursor = %d, want 11 (advances past the event either way)", cursor)
	}
	if runtime.GOOS != "windows" {
		if len(ops) != 1 || ops[0].Type != store.OpDownload {
			t.Fatalf("ops = %+v on %s, want one download (the gate is Windows-only)", ops, runtime.GOOS)
		}
		return
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none for a name Windows cannot spell verbatim", ops)
	}
	if _, err := st.GetBaseline(1, "notes."); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline keyed at the unrepresentable path: err = %v, want ErrNotFound", err)
	}
	if _, err := st.GetBaseline(1, "notes"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline keyed at the STRIPPED path: err = %v, want ErrNotFound", err)
	}
	// The drain has nothing to do and the disk stays empty: no mangled file materialised.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 0 {
		t.Errorf("view directory holds %d entries, want none (nothing may be applied)", len(ents))
	}
}

// TestReconcileUnrepresentableTwinDoesNotContaminate is the red test for an unrepresentable
// twin - the cross-contamination arc. The server holds BOTH "foo" (synced here, healthy) and
// "foo.". Before the gate, applying the "foo." create Lstat-aliased onto the healthy "foo": the
// never-lose funnel carried that innocent file off to a _Conflict copy, the promote wrote "foo."'s
// bytes into "foo"'s entry, and the next scan pushed them over the server's "foo" and emitted an
// OpRemove for the "vanished" "foo." whose divergence precondition passed - two server nodes
// destroyed with no user action. Now the twin event is skipped: "foo" keeps its content, its
// synced baseline and its identity, and no _Conflict copy is made.
func TestReconcileUnrepresentableTwinDoesNotContaminate(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the twin only aliases where the host strips trailing dots; on Unix both names coexist")
	}
	e, st, viewDir := newManglingEngine(t)
	foo := filepath.Join(viewDir, "foo")
	if err := os.WriteFile(foo, []byte("foo-content"), 0o600); err != nil {
		t.Fatalf("seed foo: %v", err)
	}
	if err := st.PutBaseline(1, "foo", store.Baseline{
		FileID: "ffoo", FileType: "file", ServerHash: "hfoo", LocalHash: "hfoo",
		LocalSize: 11, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	events := []actions.Event{
		{SyncID: 21, Action: "create", FileID: "fdot", Path: "foo.", Hash: "hdot", Size: 11, MTime: 100},
	}
	ops, _, err := e.reconcile(1, events, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("ops = %+v, want none: the twin must never be applied", ops)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	got, err := os.ReadFile(foo)
	if err != nil {
		t.Fatalf("read foo: %v", err)
	}
	if string(got) != "foo-content" {
		t.Errorf("foo content = %q, want %q (the healthy sibling must not be overwritten)", got, "foo-content")
	}
	b, err := st.GetBaseline(1, "foo")
	if err != nil {
		t.Fatalf("GetBaseline foo: %v", err)
	}
	if b.FileID != "ffoo" || b.State != store.StateSynced {
		t.Errorf("foo baseline = %+v, want the synced ffoo record untouched", b)
	}
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 {
		t.Errorf("view directory holds %d entries, want only foo (no _Conflict copy, no twin): %v",
			len(ents), dirNames(ents))
	}
	confs, err := st.ListConflicts(1)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(confs) != 0 {
		t.Errorf("conflicts = %+v, want none (the healthy sibling must not be set aside)", confs)
	}
}

func dirNames(ents []os.DirEntry) []string {
	out := make([]string, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// TestReconcileSkipsUnrepresentableMirrors covers the sibling verbs the mirror sweep found: a
// directory create (inbound_mkdir would create the stripped directory and split every descendant
// baseline) and a move whose destination is mangled (inbound_rename lands under the stripped name
// while destExists/guardRenameDest mask the miss through the same aliasing Lstat). Both are
// screened by the one gate. A move whose SOURCE is mangled is deliberately NOT here: that one is
// rescued rather than skipped, see TestReconcileRescuesMoveOffUnrepresentablePath.
func TestReconcileSkipsUnrepresentableMirrors(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the gate is Windows-only; the Unix arm is covered by TestReconcileSkipsUnrepresentableCreate")
	}
	cases := []struct {
		name  string
		event actions.Event
		seed  func(*store.Store) error
	}{
		{
			name:  "mkdir",
			event: actions.Event{SyncID: 31, Action: "create", FileID: "d1", IsDir: true, Path: "dir."},
		},
		{
			name:  "move-destination",
			event: actions.Event{SyncID: 32, Action: "move", FileID: "f1", FromPath: "notes.txt", Path: "notes."},
			seed: func(st *store.Store) error {
				return st.PutBaseline(1, "notes.txt", store.Baseline{
					FileID: "f1", FileType: "file", State: store.StateSynced,
				})
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e, st, _ := newManglingEngine(t)
			if c.seed != nil {
				if err := c.seed(st); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			ops, cursor, err := e.reconcile(1, []actions.Event{c.event}, 0)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if len(ops) != 0 {
				t.Errorf("ops = %+v, want none for an unrepresentable path", ops)
			}
			if cursor != c.event.SyncID {
				t.Errorf("cursor = %d, want %d (advances past the skipped event)", cursor, c.event.SyncID)
			}
			for _, p := range []string{c.event.Path, strings.TrimRight(c.event.Path, ". ")} {
				if _, err := st.GetBaseline(1, p); !errors.Is(err, store.ErrNotFound) {
					t.Errorf("baseline written at %q: err = %v, want ErrNotFound", p, err)
				}
			}
		})
	}
}

// TestReconcileRescuesMoveOffUnrepresentablePath pins the convergence of the remedy the gate
// itself documents (namemangle.go, DESIGN §6): "rename it on the server or on a peer that can
// represent it". The server answers such a rename with a SINGLE move event whose FromPath is the
// name this host still cannot spell - and eventContainable rejects on either endpoint, so the
// whole event used to be skipped. The operator followed the instructions and the file stayed
// permanently missing from the replica, with one Warn and no dead-letter trace.
//
// The rescue rewrites it as a create at the (perfectly containable) destination: the source was
// never materialised or tracked here, so there is nothing to relocate, and what is left is a node
// the server holds at a name we CAN represent. Both kinds are covered - a file becomes a
// download, a directory an mkdir - and a NON-representability reason for the source (an absolute
// path) rescues identically, since the argument is about the source never having been tracked,
// not about which lexical check rejected it.
//
// Every case states the node's kind on the wire (KindReported), which the rescue requires: a
// create is dispatched on ev.IsDir alone and a sparse move reports it false whatever the node
// really is, so a kind-less move is left skipped (TestRescueUncontainableSourceGate).
func TestReconcileRescuesMoveOffUnrepresentablePath(t *testing.T) {
	cases := []struct {
		name     string
		event    actions.Event
		winOnly  bool
		wantType store.OpType
		wantPath string
	}{
		{
			name:     "unrepresentable-file-source",
			event:    actions.Event{SyncID: 41, Action: "move", FileID: "fdot", KindReported: true, FromPath: "notes.", Path: "notes.txt", Hash: "hdot", Size: 11, MTime: 100},
			winOnly:  true,
			wantType: store.OpDownload,
			wantPath: "notes.txt",
		},
		{
			name:     "unrepresentable-dir-source",
			event:    actions.Event{SyncID: 42, Action: "move", FileID: "d1", IsDir: true, KindReported: true, FromPath: "Team.", Path: "Team", MTime: 100},
			winOnly:  true,
			wantType: store.OpMkdir,
			wantPath: "Team",
		},
		{
			name:     "absolute-source",
			event:    actions.Event{SyncID: 43, Action: "move", FileID: "ffoo", KindReported: true, FromPath: "/elsewhere/foo", Path: "foo.txt", Hash: "hfoo", Size: 11, MTime: 100},
			wantType: store.OpDownload,
			wantPath: "foo.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.winOnly && runtime.GOOS != "windows" {
				t.Skip("the representability gate is Windows-only, so this source is containable elsewhere")
			}
			e, st, _ := newManglingEngine(t)
			ops, cursor, err := e.reconcile(1, []actions.Event{c.event}, 0)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if cursor != c.event.SyncID {
				t.Errorf("cursor = %d, want %d", cursor, c.event.SyncID)
			}
			if len(ops) != 1 {
				t.Fatalf("ops = %+v, want exactly one op materialising the node at its new path", ops)
			}
			op := ops[0]
			if op.Type != c.wantType || op.Path != c.wantPath {
				t.Errorf("op = %s %q, want %s %q", op.Type, op.Path, c.wantType, c.wantPath)
			}
			if op.Direction != store.DirInbound || op.FileID != c.event.FileID {
				t.Errorf("op direction/file id = %v/%q, want inbound/%q", op.Direction, op.FileID, c.event.FileID)
			}
			// Enqueue-before-baseline still holds, and the record lands at the destination -
			// never at the name this host cannot spell.
			b, err := st.GetBaseline(1, c.wantPath)
			if err != nil {
				t.Fatalf("GetBaseline(%q): %v", c.wantPath, err)
			}
			if b.State != store.StatePending || b.FileID != c.event.FileID {
				t.Errorf("baseline = %+v, want a pending record for %q", b, c.event.FileID)
			}
			if _, err := st.GetBaseline(1, c.event.FromPath); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("baseline written at the non-containable source %q: err = %v, want ErrNotFound",
					c.event.FromPath, err)
			}
		})
	}
}

// TestReconcileStillSkipsMoveOntoUnrepresentablePath pins the other asymmetry: the rescue is
// strictly one-directional. A node moving ONTO a name this host cannot spell is LEAVING, and the
// only applies available would be a delete or a rename to a path that cannot exist - so the event
// is still skipped whole, leaving the local copy untouched (the never-lose direction).
func TestReconcileStillSkipsMoveOntoUnrepresentablePath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the representability gate is Windows-only")
	}
	e, st, _ := newManglingEngine(t)
	if err := st.PutBaseline(1, "notes.txt", store.Baseline{
		FileID: "fdot", FileType: "file", State: store.StateSynced, LocalHash: "hdot",
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	ops, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 51, Action: "move", FileID: "fdot", FromPath: "notes.txt", Path: "notes."},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none: the destination is the unrepresentable end", ops)
	}
	b, err := st.GetBaseline(1, "notes.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("source baseline state = %v, want it left StateSynced and untouched", b.State)
	}
}

// TestReconcileSkipsSparseMoveWithNoSourcePath is the regression for the rescue's other
// evidence hole: containable("") is false, so a move whose from_path the server simply did not
// report satisfied the "the source is non-containable" test - on a proof ("this client can never
// have tracked it") that an unreported path supports in no way. The node is normally tracked
// here, at the path the server moved it off.
//
// The damage is worst for a directory, which is what this pins: rewritten as a create, the mkdir's
// markSynced lands at the destination and PutBaseline's byid move-detection drops the SOURCE
// record, leaving the old directory on disk untracked with its children still tracked beneath it -
// which the next local scan pushes back to the server as a duplicate subtree. The event must be
// skipped whole, exactly as it was before the rescue existed: the cursor still advances, and the
// source keeps its synced baseline.
func TestReconcileSkipsSparseMoveWithNoSourcePath(t *testing.T) {
	e, st, _ := newManglingEngine(t)
	if err := st.PutBaseline(1, "Team", store.Baseline{
		FileID: "d1", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	ops, cursor, err := e.reconcile(1, []actions.Event{
		{SyncID: 61, Action: "move", FileID: "d1", IsDir: true, KindReported: true, Path: "Squad"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none: the event reported no source path to prove anything about", ops)
	}
	if cursor != 61 {
		t.Errorf("cursor = %d, want 61 (advances past the skipped event)", cursor)
	}
	if _, err := st.GetBaseline(1, "Squad"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline written at the rescued destination: err = %v, want ErrNotFound", err)
	}
	b, err := st.GetBaseline(1, "Team")
	if err != nil {
		t.Fatalf("GetBaseline(Team): %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "d1" {
		t.Errorf("source baseline = %+v, want it left StateSynced and tracked as d1", b)
	}
}

// TestRescueUncontainableSourceGate is the unit-level matrix for the rewrite's own preconditions:
// only a move, only when the source is non-containable and the destination is containable, only
// when the event states the node's KIND, and the rebuilt event never carries the source forward.
//
// The kind precondition is the one that is not merely structural. opForEvent turns a create into
// an OpMkdir or an OpDownload on ev.IsDir alone, and a sparse move reports IsDir false whatever
// the node really is - so rescuing without the evidence would enqueue a download against a
// directory's file id, which the server rejects on every drain, which no inbound escalation ever
// dead-letters, which blocks every path-overlapping op behind it forever.
func TestRescueUncontainableSourceGate(t *testing.T) {
	cases := []struct {
		name string
		ev   actions.Event
		want bool
	}{
		{"non-move", actions.Event{Action: "create", Path: "a.txt", KindReported: true}, false},
		{"both containable", actions.Event{Action: "move", FromPath: "a.txt", Path: "b.txt", KindReported: true}, false},
		{"both non-containable", actions.Event{Action: "move", FromPath: "../a", Path: "../b", KindReported: true}, false},
		{"destination non-containable", actions.Event{Action: "move", FromPath: "a.txt", Path: "../b", KindReported: true}, false},
		{"source non-containable", actions.Event{Action: "move", FromPath: "../a", Path: "b.txt", KindReported: true}, true},
		{"source escapes via dotdot", actions.Event{Action: "move", FromPath: "sub/../../a", Path: "b.txt", KindReported: true}, true},
		{"kind not reported", actions.Event{Action: "move", FromPath: "../a", Path: "b.txt"}, false},
		{"kind not reported, dir", actions.Event{Action: "move", FromPath: "../a", Path: "b.txt", IsDir: true}, false},
		// containable("") is false, so an UNREPORTED source would pass the "non-containable
		// source" test on a proof that says nothing about it - the node is usually tracked here,
		// at the path the server moved it off. Skipped exactly as before the rescue existed.
		{"source not reported", actions.Event{Action: "move", Path: "b.txt", KindReported: true}, false},
		{"source not reported, dir", actions.Event{Action: "move", Path: "b.txt", IsDir: true, KindReported: true}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := rescueUncontainableSource(c.ev)
			if ok != c.want {
				t.Fatalf("rescueUncontainableSource(%+v) ok = %v, want %v", c.ev, ok, c.want)
			}
			if !ok {
				return
			}
			if got.Action != "create" || got.Path != c.ev.Path {
				t.Errorf("rescued = %s at %q, want create at %q", got.Action, got.Path, c.ev.Path)
			}
			if got.FromPath != "" || got.FromFileID != "" {
				t.Errorf("rescued still carries the source: from %q/%q", got.FromPath, got.FromFileID)
			}
		})
	}
}

// TestReconcileSkipsKindLessMoveOffUnrepresentablePath is the wedge guard on the rescue. A move
// event that names no kind ("is_dir"/"type" absent) decodes as IsDir false, and opForEvent turns
// a create with IsDir false into an OpDownload - so rescuing it would push a download against
// what may be a DIRECTORY's file id. The server rejects that turn every drain, inbound ops are
// excluded from the deterministic-rejection dead-letter escalation, and serialize.go blocks every
// path-overlapping op behind it: a permanent wedge where the pre-rescue behaviour was inert.
// Without the kind evidence the event stays skipped, exactly as before the rescue existed.
func TestReconcileSkipsKindLessMoveOffUnrepresentablePath(t *testing.T) {
	e, st, _ := newManglingEngine(t)
	ops, cursor, err := e.reconcile(1, []actions.Event{
		// No KindReported: the wire carried neither "is_dir" nor "type".
		{SyncID: 61, Action: "move", FileID: "d1", FromPath: "/elsewhere/Team", Path: "Team"},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none: the node's kind is unknown, so neither a download nor an mkdir is provable", ops)
	}
	if cursor != 61 {
		t.Errorf("cursor = %d, want 61 (the skipped event is still acknowledged)", cursor)
	}
	if _, err := st.GetBaseline(1, "Team"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("baseline written at %q: err = %v, want ErrNotFound", "Team", err)
	}
}

// TestRescuedCreateSeesBatchPrePasses pins the rescue's PLACEMENT, not its verdict. The rewrite
// runs once up front, before the batch-wide passes, because every one of them gates on
// eventContainable - which is false for the move's original shape. Rewriting inside the per-event
// loop left the rescued create invisible to pendingOutboundUploads, so the keep-both DEFERRAL
// against a queued outbound upload at the destination never fired and the download ran instead,
// converting a clean deferral into a _Conflict copy plus churn. The reachable trigger is the
// documented remedy itself: the operator renames "notes." to a "notes.txt" the user has also
// just edited locally.
func TestRescuedCreateSeesBatchPrePasses(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the representability gate is Windows-only, so this source is containable elsewhere")
	}
	e, st, _ := newManglingEngine(t)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound,
		ViewID: 1, Path: "notes.txt", FileID: "fdot",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ops, _, err := e.reconcile(1, []actions.Event{
		{SyncID: 71, Action: "move", FileID: "fdot", KindReported: true,
			FromPath: "notes.", Path: "notes.txt", Hash: "hdot", Size: 11, MTime: 100},
	}, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("ops = %+v, want none: the queued upload at the destination defers the download to the keep-both resolver", ops)
	}
}

// TestUnrepresentablePathDeadLettersAtApply pins the defence-in-depth layer: an op that predates
// the reconcile screen (queued by an older build, or by cold start) reaches abs(), which refuses
// it as errRejectedPath so applyOp dead-letters it with an observable trace instead of letting
// the mangled apply land. On Unix the same op applies normally - the gate is Windows-only.
func TestUnrepresentablePathDeadLettersAtApply(t *testing.T) {
	e, st, viewDir := newManglingEngine(t)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 1, Path: "notes.", FileID: "fdot", UnixPerm: 0o644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if runtime.GOOS != "windows" {
		if n, err := st.DeadLetterLen(); err != nil || n != 0 {
			t.Fatalf("DeadLetterLen = %d (err %v) on %s, want 0 (the gate is Windows-only)",
				n, err, runtime.GOOS)
		}
		if _, err := os.Lstat(filepath.Join(viewDir, "notes.")); err != nil {
			t.Errorf("notes. not applied on %s: %v", runtime.GOOS, err)
		}
		return
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want the op dropped, not retried forever", n, err)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1 with an observable trace", len(dls))
	}
	if dls[0].Path != "notes." {
		t.Errorf("dead letter path = %q, want %q", dls[0].Path, "notes.")
	}
	if dls[0].Reason == "" {
		t.Error("dead letter carries no reason; the trace must name why the path is unappliable")
	}
	// Neither spelling may exist: the drain must not have created the stripped twin.
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 0 {
		t.Errorf("view directory holds %v, want nothing applied", dirNames(ents))
	}
}

// TestResolveConflictRejectsUnrepresentablePath is the keep-both mirror: the canonical promote
// would strip the same way, and conflictCopyName for "notes." would park the copy under a name
// different from the durable Conflict record's. The resolver's own containment guard shares
// containable(), so the one gate closes this arc too - the op dead-letters rather than parking
// content under a name nothing describes.
func TestResolveConflictRejectsUnrepresentablePath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the gate is Windows-only; on Unix such a name is legitimate and resolves normally")
	}
	e, _, _ := newManglingEngine(t)
	err := e.resolveConflict(context.Background(), store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound,
		ViewID: 1, Path: "notes.", FileID: "fdot",
	})
	if !errors.Is(err, errRejectedPath) {
		t.Errorf("resolveConflict(%q) = %v, want errRejectedPath", "notes.", err)
	}
}

// TestAbsRejectsUnrepresentableViewName is the view-name arm of the same gate: a view whose own
// name Windows would strip must not resolve to a directory at all, since its whole subtree would
// materialise under a spelling no baseline key describes.
func TestAbsRejectsUnrepresentableViewName(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Team."}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, t.TempDir(), "sess", nil)
	got, err := e.abs(7, "a.txt")
	if runtime.GOOS != "windows" {
		if err != nil {
			t.Fatalf("abs on %s: %v, want the name accepted verbatim", runtime.GOOS, err)
		}
		if filepath.Base(filepath.Dir(got)) != "Team." {
			t.Errorf("abs = %q, want it under the verbatim %q directory", got, "Team.")
		}
		return
	}
	if !errors.Is(err, errRejectedPath) {
		t.Errorf("abs with view name %q: err = %v, want errRejectedPath", "Team.", err)
	}
}

// TestResolveViewDirRejectsUnrepresentableViewName is the SCAN arm of the view-name gate, the
// one abs() cannot cover. resolveViewDir is what every local scan, mount-swap probe and
// device-id read resolves through, and it validated containment only. For a view named "Team."
// Win32 aliases syncRoot\Team. onto syncRoot\Team, so the scan walked a DIFFERENT directory - a
// sibling view's, or unrelated user content - and surfaced everything in it as an untracked local
// create of this view. Uploads of those creates dead-letter at abs(), but applyMkdirOut validates
// only the view-relative path, so the aliased tree's directories would have been pushed into this
// view server-side. The scan is suppressed instead.
func TestResolveViewDirRejectsUnrepresentableViewName(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Team."}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	// The stripped twin exists on disk, which is exactly what an aliasing resolve would find.
	if err := os.MkdirAll(filepath.Join(root, "Team"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", nil)
	_, viewAbs, err := e.resolveViewDir(7)
	if runtime.GOOS != "windows" {
		if err != nil {
			t.Fatalf("resolveViewDir on %s: %v, want the name accepted verbatim", runtime.GOOS, err)
		}
		if filepath.Base(viewAbs) != "Team." {
			t.Errorf("resolveViewDir = %q, want the verbatim %q directory", viewAbs, "Team.")
		}
		return
	}
	if err == nil {
		t.Fatalf("resolveViewDir = %q, want the unrepresentable view name refused", viewAbs)
	}
	// scanView is the caller that matters: it must refuse rather than walk the aliased twin.
	if _, _, serr := e.scanView(context.Background(), 7, nil); serr == nil {
		t.Error("scanView succeeded for an unrepresentable view name; want the scan suppressed")
	}
}

// TestViewNameSubdirRejectsUnrepresentableName is the view-rename mirror: the adoption matrix
// must not run for a name the host strips. Its Lstat probes of the new name alias the STRIPPED
// directory - for "Team" -> "Team." that is the old directory itself - so the probes would
// "confirm" a rename that never happened and re-point the view's scan at a name no baseline
// describes. A false verdict here makes adoptViewRename defer with a Warn instead.
func TestViewNameSubdirRejectsUnrepresentableName(t *testing.T) {
	windows := runtime.GOOS == "windows"
	for _, name := range []string{"Team.", "Team ", "a./b"} {
		rel, ok := viewNameSubdir(name)
		if ok == windows {
			t.Errorf("viewNameSubdir(%q) on %s = (%q, %v), want ok = %v",
				name, runtime.GOOS, rel, ok, !windows)
		}
	}
	if _, ok := viewNameSubdir("Team"); !ok {
		t.Error("viewNameSubdir(\"Team\") rejected; an ordinary name must still map")
	}
}
