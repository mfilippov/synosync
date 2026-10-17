package engine

// Tests for the 8.3 short-name alias gate: a legal server-side name spelled
// like an NTFS short name - "DOCUME~1.TXT" is a name any Linux or macOS peer may create - must
// never be applied on a Windows client where it resolves onto a DIFFERENT, already-tracked local
// file. Without the gate the fresh-create arm read the neighbour's content as an untracked
// occupant, the never-lose funnel carried that healthy file off to a _Conflict copy, and the
// next scan pushed an OpRemove that destroyed the server's long-named node for every client.
//
// The empirical arms probe the host: short-name generation is a per-VOLUME setting (on by
// default for the system volume, frequently off for data disks), so hostShortName decides
// whether the fixture can run rather than a runtime.GOOS branch. The seam-driven arms wire
// e.longPath and therefore carry the same coverage on every platform, including a Windows
// volume with 8dot3name disabled.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

const (
	longNeighbourName = "Document Name Long.txt"
	longNeighbourDir  = "Subdirectory Long Name"
)

// TestHostAliasesShortNameOnDisk pins the premise the gate rests on, on the real filesystem: the
// auto-generated 8.3 alias of a long-named file is a fully addressable second name for the SAME
// directory entry, and Lstat echoes back whichever spelling it was handed - so nothing in a
// path-addressed gate can tell that it answered for the neighbour.
func TestHostAliasesShortNameOnDisk(t *testing.T) {
	dir := t.TempDir()
	long := filepath.Join(dir, longNeighbourName)
	if err := os.WriteFile(long, []byte("neighbour-content"), 0o600); err != nil {
		t.Fatalf("seed long-named file: %v", err)
	}
	short, ok := hostShortName(t, long)
	if !ok {
		t.Skip("host generates no distinct 8.3 alias here (non-Windows, or 8dot3name disabled on this volume)")
	}
	shortAbs := filepath.Join(dir, short)
	got, err := os.ReadFile(shortAbs)
	if err != nil || string(got) != "neighbour-content" {
		t.Fatalf("ReadFile(%q) = %q (err %v), want the long-named file's bytes", short, got, err)
	}
	fi, err := os.Lstat(shortAbs)
	if err != nil {
		t.Fatalf("Lstat(%q): %v", short, err)
	}
	if fi.Name() != short {
		t.Errorf("Lstat(%q).Name() = %q; the alias is expected to echo back verbatim, which is how the engine is deceived",
			short, fi.Name())
	}
	// And the resolver the gate uses reports the truth the stat cannot.
	real, err := osLongPathName(shortAbs)
	if err != nil {
		t.Fatalf("osLongPathName(%q): %v", short, err)
	}
	if filepath.Base(real) != longNeighbourName {
		t.Errorf("osLongPathName(%q) = %q, want it to de-alias to %q", short, real, longNeighbourName)
	}
}

// TestAliasedComponent is the comparison's truth table. A short name never survives folding; a
// case-only or NFC/NFD difference must NOT be read as an alias (a case-insensitive volume answers
// with whatever case it was handed, and the NFC-tolerant resolution reaches the same entry).
func TestAliasedComponent(t *testing.T) {
	root := filepath.Join(string(filepath.Separator)+"srv", "root")
	cases := []struct {
		name       string
		addressed  string
		real       string
		depth      int
		wantAlias  bool
		wantActual string
	}{
		{
			name:      "identical",
			addressed: filepath.Join(root, "Photos", "a.txt"),
			real:      filepath.Join(root, "Photos", "a.txt"),
			depth:     2,
		},
		{
			name:      "case only",
			addressed: filepath.Join(root, "Photos", "A.TXT"),
			real:      filepath.Join(root, "photos", "a.txt"),
			depth:     2,
		},
		{
			name:      "nfd vs nfc",
			addressed: filepath.Join(root, "Photos", "é.txt"),
			real:      filepath.Join(root, "Photos", "é.txt"),
			depth:     2,
		},
		{
			name:       "leaf alias",
			addressed:  filepath.Join(root, "Photos", "DOCUME~1.TXT"),
			real:       filepath.Join(root, "Photos", longNeighbourName),
			depth:      2,
			wantAlias:  true,
			wantActual: longNeighbourName,
		},
		{
			name:       "parent alias",
			addressed:  filepath.Join(root, "SUBDIR~1", "a.txt"),
			real:       filepath.Join(root, longNeighbourDir, "a.txt"),
			depth:      2,
			wantAlias:  true,
			wantActual: longNeighbourDir,
		},
		{
			name:      "root component differs above the compared depth",
			addressed: filepath.Join(root, "Photos", "a.txt"),
			real:      filepath.Join(string(filepath.Separator)+"srv", "MFILIP~1", "Photos", "a.txt"),
			depth:     2,
		},
		{
			name:      "real spelling too shallow to compare",
			addressed: filepath.Join(root, "Photos", "a.txt"),
			real:      "a.txt",
			depth:     2,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, actual, ok := aliasedComponent(c.addressed, c.real, c.depth)
			if ok != c.wantAlias {
				t.Fatalf("aliasedComponent(%q, %q, %d) alias = %v, want %v",
					c.addressed, c.real, c.depth, ok, c.wantAlias)
			}
			if ok && actual != c.wantActual {
				t.Errorf("actual spelling = %q, want %q", actual, c.wantActual)
			}
		})
	}
}

// aliasSeam models GetLongPathName over the real disk: a path addressed through the alias
// resolves to the real spelling WHILE that entity exists, and any path that does not exist
// reports the not-exist error the gate walks up from. Wiring it makes the gate exercisable on
// hosts that generate no short names at all.
func aliasSeam(dir, alias, real string) func(string) (string, error) {
	aliasAbs := filepath.Join(dir, alias)
	realAbs := filepath.Join(dir, real)
	return func(name string) (string, error) {
		resolved := name
		if name == aliasAbs || strings.HasPrefix(name, aliasAbs+string(filepath.Separator)) {
			if _, err := os.Lstat(realAbs); err == nil {
				resolved = realAbs + name[len(aliasAbs):]
			}
		}
		if _, err := os.Lstat(resolved); err != nil {
			return "", err
		}
		return resolved, nil
	}
}

// TestRejectShortNameAliasWalksToExistingPrefix pins the walk itself: the trailing components of
// a materialising apply do not exist yet, so the gate must climb to the longest EXISTING prefix
// and compare there - and must stay silent about the sync root's own ancestry, which may
// legitimately be short-named.
func TestRejectShortNameAliasWalksToExistingPrefix(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, longNeighbourDir), 0o755); err != nil {
		t.Fatalf("seed directory: %v", err)
	}
	e := &Engine{syncRoot: root, longPath: aliasSeam(root, "SUBDIR~1", longNeighbourDir)}

	// A not-yet-existing leaf under an ALIASED parent is still refused: the parent is the
	// component that would graft the subtree into a stranger's directory.
	err := e.rejectShortNameAlias(filepath.Join(root, "SUBDIR~1", "deep", "a.txt"))
	if !errors.Is(err, errRejectedPath) {
		t.Errorf("aliased parent with a missing leaf: err = %v, want errRejectedPath", err)
	}
	// A not-yet-existing leaf under a real parent is fine.
	if err := e.rejectShortNameAlias(filepath.Join(root, longNeighbourDir, "a.txt")); err != nil {
		t.Errorf("real parent with a missing leaf: err = %v, want nil", err)
	}
	// The sync root itself and a path outside it are never judged.
	if err := e.rejectShortNameAlias(root); err != nil {
		t.Errorf("sync root: err = %v, want nil", err)
	}
	if err := e.rejectShortNameAlias(filepath.Join(filepath.Dir(root), "elsewhere")); err != nil {
		t.Errorf("path outside the sync root: err = %v, want nil", err)
	}
	// A resolver that cannot answer (anything but "no such path") is inconclusive, not a
	// rejection: short names are exposed through the very call that failed, so a filesystem
	// refusing it is not one that aliases - and failing closed would dead-letter every
	// materialising op on such a mount.
	e.longPath = func(string) (string, error) { return "", os.ErrPermission }
	if err := e.rejectShortNameAlias(filepath.Join(root, "SUBDIR~1", "a.txt")); err != nil {
		t.Errorf("inconclusive resolver: err = %v, want nil (apply proceeds)", err)
	}
}

// aliasEngine builds a real-disk engine over one view whose directory holds a long-named
// neighbour, with the alias seam wired so the fixture runs on every host.
func aliasEngine(t *testing.T, alias, real string, seed []byte) (*Engine, *store.Store, string) {
	t.Helper()
	c := &pushClient{
		downloadContent: map[string][]byte{"fshort": []byte("short-name-node")},
		downloadNode: map[string]actions.Node{
			"fshort": {FileID: "fshort", Hash: md4.ContentHash([]byte("short-name-node")), MTime: 555,
				Size: int64(len("short-name-node"))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if seed != nil {
		if err := os.WriteFile(filepath.Join(viewDir, real), seed, 0o600); err != nil {
			t.Fatalf("seed neighbour %q: %v", real, err)
		}
		if err := st.PutBaseline(7, real, store.Baseline{
			FileID: "flong", FileType: "file", ServerHash: md4.ContentHash(seed),
			LocalHash: md4.ContentHash(seed), LocalSize: int64(len(seed)), State: store.StateSynced,
		}); err != nil {
			t.Fatalf("seed neighbour baseline: %v", err)
		}
	}
	e.longPath = aliasSeam(viewDir, alias, real)
	return e, st, viewDir
}

// TestInboundCreateOnShortNameAliasDeadLetters is the red test. Before the gate this drain
// carried the healthy, synced "Document Name Long.txt" off to a _Conflict copy and wrote the
// short-named node's bytes into its entry - the first two steps of the arc that ends with the
// server's long-named file deleted for every client. Now the op dead-letters with a trace and
// the neighbour is untouched in content, baseline and identity.
func TestInboundCreateOnShortNameAliasDeadLetters(t *testing.T) {
	neighbour := []byte("neighbour-content")
	e, st, viewDir := aliasEngine(t, "DOCUME~1.TXT", longNeighbourName, neighbour)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "DOCUME~1.TXT", FileID: "fshort", UnixPerm: 0o644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
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
	if dls[0].Path != "DOCUME~1.TXT" || dls[0].Reason == "" {
		t.Errorf("dead letter = %+v, want the aliased path with a reason", dls[0])
	}
	got, err := os.ReadFile(filepath.Join(viewDir, longNeighbourName))
	if err != nil || string(got) != string(neighbour) {
		t.Fatalf("neighbour content = %q (err %v), want %q untouched", got, err, neighbour)
	}
	b, err := st.GetBaseline(7, longNeighbourName)
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.FileID != "flong" || b.State != store.StateSynced {
		t.Errorf("neighbour baseline = %+v, want the synced flong record untouched", b)
	}
	confs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(confs) != 0 {
		t.Errorf("conflicts = %+v, want none (the healthy neighbour must not be set aside)", confs)
	}
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 {
		t.Errorf("view directory holds %v, want only the neighbour", dirNames(ents))
	}
}

// TestInboundCreateOnHostShortNameAliasDeadLetters is the same red test with NO seam: the alias
// is the one the host itself generated and the gate runs against the real GetLongPathName. It
// skips where the volume generates no distinct short name, which is also the "no 8.3 here"
// control - the arm below proves such a host applies the name normally.
func TestInboundCreateOnHostShortNameAliasDeadLetters(t *testing.T) {
	neighbour := []byte("neighbour-content")
	e, st, viewDir := aliasEngine(t, "unused", longNeighbourName, neighbour)
	e.longPath = nil // exercise the per-OS resolver against the real filesystem

	short, ok := hostShortName(t, filepath.Join(viewDir, longNeighbourName))
	if !ok {
		t.Skip("host generates no distinct 8.3 alias here (non-Windows, or 8dot3name disabled on this volume)")
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: short, FileID: "fshort", UnixPerm: 0o644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 1: %q aliases %q on this volume",
			n, err, short, longNeighbourName)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, longNeighbourName))
	if err != nil || string(got) != string(neighbour) {
		t.Fatalf("neighbour content = %q (err %v), want %q untouched", got, err, neighbour)
	}
	if confs, cerr := st.ListConflicts(7); cerr != nil || len(confs) != 0 {
		t.Errorf("conflicts = %+v (err %v), want none", confs, cerr)
	}
}

// TestShortNameShapedCreateWithoutAliasApplies is the control: the very same 8.3-SHAPED name,
// with no long-named neighbour for it to alias. Nothing resolves differently, so the gate must
// stay out of the way and the node must land verbatim - on a volume with 8dot3name disabled that
// is every such name, which is what makes this the "no 8.3 here, behaviour unchanged" arm.
func TestShortNameShapedCreateWithoutAliasApplies(t *testing.T) {
	e, st, viewDir := aliasEngine(t, "DOCUME~1.TXT", longNeighbourName, nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "DOCUME~1.TXT", FileID: "fshort", UnixPerm: 0o644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 0: nothing aliases here", n, err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "DOCUME~1.TXT"))
	if err != nil || string(got) != "short-name-node" {
		t.Fatalf("applied content = %q (err %v), want the server node landed verbatim", got, err)
	}
	b, err := st.GetBaseline(7, "DOCUME~1.TXT")
	if err != nil || b.State != store.StateSynced {
		t.Errorf("baseline = %+v (err %v), want a synced record at the literal name", b, err)
	}
}

// TestInboundMkdirOnShortNameAliasDeadLetters is the mkdir mirror: os.MkdirAll no-ops onto the
// aliased neighbour directory, after which every child of the inbound subtree is grafted INSIDE
// a stranger's directory while the baselines describe a path that does not exist.
func TestInboundMkdirOnShortNameAliasDeadLetters(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.Mkdir(filepath.Join(viewDir, longNeighbourDir), 0o755); err != nil {
		t.Fatalf("seed neighbour directory: %v", err)
	}
	if err := st.PutBaseline(7, longNeighbourDir, store.Baseline{
		FileID: "dlong", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed neighbour baseline: %v", err)
	}
	e.longPath = aliasSeam(viewDir, "SUBDIR~1", longNeighbourDir)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound,
		ViewID: 7, Path: "SUBDIR~1", FileID: "dshort", UnixPerm: 0o755,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 1", n, err)
	}
	ents, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 1 {
		t.Errorf("view directory holds %v, want only the neighbour directory", dirNames(ents))
	}
	b, err := st.GetBaseline(7, longNeighbourDir)
	if err != nil || b.FileID != "dlong" {
		t.Errorf("neighbour baseline = %+v (err %v), want the synced dlong record untouched", b, err)
	}
}

// TestInboundRenameOntoShortNameAliasDeadLetters is the rename mirror: the destination aliases a
// tracked neighbour, so guardRenameDest reasoned about a stranger's file - either preserving it
// as a _Conflict copy or replacing it outright, depending on whose hash the divergence gate
// happened to compare. The gate refuses the destination before either can happen; the source
// node stays in place with its baseline intact, so the next event re-derives the move.
func TestInboundRenameOntoShortNameAliasDeadLetters(t *testing.T) {
	neighbour := []byte("neighbour-content")
	moved := []byte("moved-node-content")
	e, st, viewDir := aliasEngine(t, "DOCUME~1.TXT", longNeighbourName, neighbour)
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), moved, 0o600); err != nil {
		t.Fatalf("seed source file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fmov", FileType: "file", ServerHash: md4.ContentHash(moved),
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed source baseline: %v", err)
	}

	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirInbound, ViewID: 7,
		FileID: "fmov", Path: "a.txt", NewPath: "DOCUME~1.TXT",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 1", n, err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, longNeighbourName))
	if err != nil || string(got) != string(neighbour) {
		t.Fatalf("neighbour content = %q (err %v), want %q untouched", got, err, neighbour)
	}
	src, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil || string(src) != string(moved) {
		t.Fatalf("source content = %q (err %v), want the move not to have happened", src, err)
	}
	if confs, cerr := st.ListConflicts(7); cerr != nil || len(confs) != 0 {
		t.Errorf("conflicts = %+v (err %v), want none", confs, cerr)
	}
}
