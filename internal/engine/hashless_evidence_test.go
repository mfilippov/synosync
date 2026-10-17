package engine

// Tests for the three sites where "the two sides hold the same content" was provable
// in exactly ONE form - a non-empty server-reported hash - with no degradation when the reply
// omits it (an unconfirmed reply shape, DESIGN §13). Each site now falls back to the evidence it
// still has (size+kind, size+identity) and says so with a Warn; the hash-carrying forms must
// behave byte-for-byte as before.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestColdStartContentMatchArcs pins the cold-start match verdict per evidence form: kind alone
// decides directories and type mismatches, two hashes decide files, and a missing hash on EITHER
// side degrades to size rather than asserting divergence.
func TestColdStartContentMatchArcs(t *testing.T) {
	cases := []struct {
		name         string
		ev           actions.Event
		en           scanEntry
		wantMatch    bool
		wantDegraded bool
	}{
		{
			name: "dirs match on kind",
			ev:   actions.Event{IsDir: true}, en: scanEntry{isDir: true},
			wantMatch: true,
		},
		{
			name: "type mismatch never matches",
			ev:   actions.Event{IsDir: true}, en: scanEntry{md4: "h1", size: 3},
			wantMatch: false,
		},
		{
			name: "equal hashes match without degrading",
			ev:   actions.Event{Hash: "h1", Size: 3}, en: scanEntry{md4: "h1", size: 3},
			wantMatch: true,
		},
		{
			name: "different hashes diverge even at equal size",
			ev:   actions.Event{Hash: "h1", Size: 3}, en: scanEntry{md4: "h2", size: 3},
			wantMatch: false,
		},
		{
			name: "hash-less catalog degrades to size",
			ev:   actions.Event{Hash: "", Size: 3}, en: scanEntry{md4: "h1", size: 3},
			wantMatch: true, wantDegraded: true,
		},
		{
			name: "hash-less catalog with different size stays divergent",
			ev:   actions.Event{Hash: "", Size: 5}, en: scanEntry{md4: "h1", size: 3},
			wantMatch: false, wantDegraded: true,
		},
		{
			name: "hash-less local side degrades too",
			ev:   actions.Event{Hash: "h1", Size: 3}, en: scanEntry{md4: "", size: 3},
			wantMatch: true, wantDegraded: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			matched, degraded := coldStartContentMatch(tc.ev, tc.en)
			if matched != tc.wantMatch || degraded != tc.wantDegraded {
				t.Errorf("coldStartContentMatch = (%v, %v), want (%v, %v)",
					matched, degraded, tc.wantMatch, tc.wantDegraded)
			}
		})
	}
}

// TestColdStartHashLessCatalogAdoptsInsteadOfConflictStorm is the red test for the first site: a
// catalog that carries no per-file hash used to make EVERY pre-seeded file compare unequal, so
// the most common onboarding scenario (a tree already copied into the sync root) turned into
// O(tree) keep-both resolutions with a copy plus two transfers each. The degraded verdict adopts
// the identical files on size+kind and records the LOCAL hash, so no later scan re-pushes them.
func TestColdStartHashLessCatalogAdoptsInsteadOfConflictStorm(t *testing.T) {
	e, st := newReconcileEngine(t, "self")

	hashLess := func(path string, size int64) actions.Event {
		ev := serverFile(path, "srv-"+path, "", size)
		return ev
	}
	ops, err := e.coldStartReconcile(1,
		catalogMap(hashLess("a.txt", 3), hashLess("b.txt", 5), serverDir("d", "srv-d")),
		[]scanEntry{localFile("a.txt", "h1", 3), localFile("b.txt", "h2", 5), localDir("d")},
		nil, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("enqueued %d ops, want 0 (hash-less catalog must not storm keep-both): %+v", len(ops), ops)
	}
	for _, p := range []string{"a.txt", "b.txt", "d"} {
		b, err := st.GetBaseline(1, p)
		if err != nil {
			t.Fatalf("GetBaseline %q: %v", p, err)
		}
		if b.State != store.StateSynced {
			t.Errorf("baseline %q state = %q, want synced", p, b.State)
		}
	}
	// The adopted file baseline must describe the bytes on disk: the LOCAL hash, and no
	// fabricated server hash.
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalHash != "h1" || b.ServerHash != "" || b.LocalSize != 3 {
		t.Errorf("adopted baseline = %+v, want LocalHash h1 / empty ServerHash / size 3", b)
	}
}

// TestColdStartHashLessCatalogDifferentSizeStillConflicts confirms the degrade is not a blanket
// "adopt everything": a size difference is proof of divergence even with no hash, so the path
// still funnels into the keep-both resolver.
func TestColdStartHashLessCatalogDifferentSizeStillConflicts(t *testing.T) {
	e, st := newReconcileEngine(t, "self")

	ops, err := e.coldStartReconcile(1,
		catalogMap(serverFile("a.txt", "srv1", "", 9)),
		[]scanEntry{localFile("a.txt", "h1", 3)},
		nil, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpResolveConflict {
		t.Fatalf("ops = %+v, want one OpResolveConflict", ops)
	}
	b, err := st.GetBaseline(1, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want conflict", b.State)
	}
}

// TestColdStartHashCarryingCatalogUnchanged pins that a catalog WITH hashes decides exactly as
// before: equal hashes adopt (recording the shared hash on both sides), different hashes at the
// same size still keep-both.
func TestColdStartHashCarryingCatalogUnchanged(t *testing.T) {
	e, st := newReconcileEngine(t, "self")

	ops, err := e.coldStartReconcile(1,
		catalogMap(serverFile("same.txt", "s1", "h1", 3), serverFile("diff.txt", "s2", "h9", 3)),
		[]scanEntry{localFile("same.txt", "h1", 3), localFile("diff.txt", "h8", 3)},
		nil, false)
	if err != nil {
		t.Fatalf("coldStartReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "diff.txt" || ops[0].Type != store.OpResolveConflict {
		t.Fatalf("ops = %+v, want one OpResolveConflict for diff.txt", ops)
	}
	b, err := st.GetBaseline(1, "same.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != "h1" || b.ServerHash != "h1" {
		t.Errorf("adopted baseline = %+v, want synced with h1 on both sides", b)
	}
}

// TestResolveConflictHashLessOwnNodeKeepsBoth pins the never-lose direction of the second site.
// The false-conflict fast path degrades to size+identity when the query_node
// reply omits the hash - but a MODIFY's resolve addresses its own node, so the identity half holds
// by construction and the guard collapses to a bare size comparison on a path the server has just
// PROVED diverged by rejecting the push. Another client's same-sized edit satisfies it; adopting
// records StateSynced with the LOCAL hash as the agreed base, and the next server event's
// divergence gate then finds nothing diverged and overwrites the unpushed local edit with no copy.
// So an unprovable equality resolves as the real keep-both: the local bytes survive whole.
func TestResolveConflictHashLessOwnNodeKeepsBoth(t *testing.T) {
	ts := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	body := []byte("local one")
	remote := []byte("remote  1")

	if len(body) != len(remote) {
		t.Fatalf("fixture: the two bodies must be the same size (%d vs %d)", len(body), len(remote))
	}
	c := &pushClient{
		queryNode: map[string]actions.Node{
			// The reply omits the content hash entirely, and reports OUR node at OUR byte count.
			"a.txt": {FileID: "srv1", MTime: 555, Size: int64(len(remote))},
		},
		downloadContent: map[string][]byte{"srv1": remote},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: md4.ContentHash(body),
		LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "srv1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("conflict records = %d, want 1: a hash-less reply proves no equality, so the local edit is preserved", len(recs))
	}
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(recs[0].CopyPath)))
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("copy content = %q, want the local edit %q preserved whole", got, body)
	}
	canon, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read canonical: %v", err)
	}
	if string(canon) != string(remote) {
		t.Errorf("canonical content = %q, want the server version %q", canon, remote)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictHashLessServerNodeKeepsBothOnDifferentSize confirms the degrade never
// swallows a real divergence: a hash-less reply whose SIZE differs from the local file is still
// the real conflict, so the local content is preserved as a keep-both copy.
func TestResolveConflictHashLessServerNodeKeepsBothOnDifferentSize(t *testing.T) {
	ts := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	body := []byte("local edit")
	remote := []byte("server version, longer")

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", MTime: 555, Size: int64(len(remote))},
		},
		downloadContent: map[string][]byte{"srv1": remote},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: md4.ContentHash(body),
		LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "srv1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("conflict records = %d, want 1 keep-both copy", len(recs))
	}
	// The local bytes survive under the copy name.
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(recs[0].CopyPath)))
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("copy content = %q, want the local edit %q", got, body)
	}
}

// TestResolveConflictHashLessForeignNodeKeepsBoth confirms the identity half of the degrade: a
// hash-less node of the SAME size but a different file id is not our node, so it is the real
// conflict rather than a false one.
func TestResolveConflictHashLessForeignNodeKeepsBoth(t *testing.T) {
	ts := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	body := []byte("identical")
	remote := []byte("intruder!")

	if len(body) != len(remote) {
		t.Fatalf("fixture: the two bodies must be the same size (%d vs %d)", len(body), len(remote))
	}
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "other", MTime: 555, Size: int64(len(remote))},
		},
		downloadContent: map[string][]byte{"other": remote},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: md4.ContentHash(body),
		LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "srv1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("conflict records = %d, want 1 (a foreign id is never adopted as ours)", len(recs))
	}
}

// TestResolveConflictHashLessIdLessResolveKeepsBoth pins the strict arm of the degrade: a resolve
// funnelled from a FRESH CREATE carries no file id (funnelUploadConflict/funnelTypeCollision copy
// the rejected op's, empty for a create), so there is no identity evidence to degrade to and a
// same-sized server node is a STRANGER, not proof of equality. Adopting it would mark the baseline
// synced over two different byte streams - and, since the baseline then records the local hash as
// the agreed base, the next server modify's divergence gate would find nothing diverged and
// overwrite the local file with no _Conflict copy. The sibling degrade in probeUploadedNode refuses
// the same shape for the same reason.
func TestResolveConflictHashLessIdLessResolveKeepsBoth(t *testing.T) {
	ts := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	body := []byte("local one")
	remote := []byte("stranger!")

	if len(body) != len(remote) {
		t.Fatalf("fixture: the two bodies must be the same size (%d vs %d)", len(body), len(remote))
	}
	c := &pushClient{
		queryNode: map[string]actions.Node{
			// Hash-less reply, exactly our byte count, but a node we have never been told is ours.
			"a.txt": {FileID: "srv1", MTime: 555, Size: int64(len(remote))},
		},
		downloadContent: map[string][]byte{"srv1": remote},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash(body),
		LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The op carries no FileID - the shape a fresh create's 24579/type-collision funnel produces.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("conflict records = %d, want 1: an id-less resolve has no identity evidence, so a same-sized node is never adopted as ours", len(recs))
	}
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(recs[0].CopyPath)))
	if err != nil {
		t.Fatalf("read copy: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("copy content = %q, want the local bytes %q preserved whole", got, body)
	}
	canon, err := os.ReadFile(filepath.Join(viewDir, "a.txt"))
	if err != nil {
		t.Fatalf("read canonical: %v", err)
	}
	if string(canon) != string(remote) {
		t.Errorf("canonical content = %q, want the server version %q", canon, remote)
	}
}

// TestProbeUploadedNodeEvidenceArcs pins the third site per evidence form: a hash-carrying reply
// decides on the hash exactly as before, and a hash-less reply is adopted only when the failure
// was the LOST-REPLY one (ErrPathError) and the node is OUR node (the modify's own file id) at
// exactly our byte count. A stale-base rejection keeps the strict hash gate: the server has stated
// our bytes did not land, so a same-sized node is another client's edit, not our success.
func TestProbeUploadedNodeEvidenceArcs(t *testing.T) {
	const wantHash = "aa11"
	const wantSize int64 = 5

	cases := []struct {
		name      string
		op        store.Op
		reply     actions.Node
		lostReply bool
		wantID    string // "" => the probe must adopt nothing
	}{
		{
			name:      "hash match adopts",
			op:        store.Op{ViewID: 7, Path: "a.txt"},
			reply:     actions.Node{FileID: "srv1", Hash: wantHash, Size: wantSize},
			lostReply: true,
			wantID:    "srv1",
		},
		{
			name:  "hash match adopts on a stale-base rejection too",
			op:    store.Op{ViewID: 7, Path: "a.txt"},
			reply: actions.Node{FileID: "srv1", Hash: wantHash, Size: wantSize},

			wantID: "srv1",
		},
		{
			name:      "hash mismatch adopts nothing",
			op:        store.Op{ViewID: 7, Path: "a.txt"},
			reply:     actions.Node{FileID: "srv1", Hash: "bb22", Size: wantSize},
			lostReply: true,
		},
		{
			name:      "hash-less modify with our id and size adopts on a lost reply",
			op:        store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply:     actions.Node{FileID: "srv1", Size: wantSize},
			lostReply: true,
			wantID:    "srv1",
		},
		{
			name:  "hash-less modify adopts nothing on a stale-base rejection",
			op:    store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply: actions.Node{FileID: "srv1", Size: wantSize},
		},
		{
			name:      "hash-less modify with a different size adopts nothing",
			op:        store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply:     actions.Node{FileID: "srv1", Size: wantSize + 1},
			lostReply: true,
		},
		{
			name:      "hash-less foreign id adopts nothing",
			op:        store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply:     actions.Node{FileID: "other", Size: wantSize},
			lostReply: true,
		},
		{
			name:      "hash-less fresh create has no identity to degrade to",
			op:        store.Op{ViewID: 7, Path: "a.txt"},
			reply:     actions.Node{FileID: "srv1", Size: wantSize},
			lostReply: true,
		},
		{
			name:      "hash-less directory is never our upload",
			op:        store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply:     actions.Node{FileID: "srv1", IsDir: true, Size: wantSize},
			lostReply: true,
		},
		{
			name:      "id-less reply adopts nothing",
			op:        store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"},
			reply:     actions.Node{Hash: wantHash, Size: wantSize},
			lostReply: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{queryNode: map[string]actions.Node{"a.txt": tc.reply}}
			e, _, _ := outboundEngine(t, c, "Photos", "root1")
			node, err := e.probeUploadedNode(context.Background(), tc.op, wantHash, wantSize, tc.lostReply)
			if err != nil {
				t.Fatalf("probeUploadedNode: %v", err)
			}
			if node.FileID != tc.wantID {
				t.Errorf("probe node id = %q, want %q", node.FileID, tc.wantID)
			}
		})
	}
}

// TestProbeUploadedNodeZeroSizeIsNoEvidence pins the one shape where the identity+size degrade
// would collapse to nothing: a truncate-to-zero push (wantSize == 0) whose probe reply omits the
// hash AND the size. Node.Size is tolerantly decoded, so the omission decodes to the same zero a
// genuine empty file reports, and identity is no counter-evidence (a modify's probe addresses its
// own node). Adopting there would record an unlanded truncation as synced and let the server's
// older content download over it with no _Conflict copy, so the degrade requires a POSITIVE size.
func TestProbeUploadedNodeZeroSizeIsNoEvidence(t *testing.T) {
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"}
	// A reply carrying an id and nothing else: no hash, no size, no mtime.
	c := &pushClient{queryNode: map[string]actions.Node{"a.txt": {FileID: "srv1"}}}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	node, err := e.probeUploadedNode(context.Background(), op, md4.ContentHash(nil), 0, true)
	if err != nil {
		t.Fatalf("probeUploadedNode: %v", err)
	}
	if node.FileID != "" {
		t.Errorf("probe adopted %q on a reply carrying neither hash nor size; want nothing adopted", node.FileID)
	}

	// A server that genuinely states a positive size still degrades, so the guard is not a blanket
	// withdrawal of the size+identity arm.
	c2 := &pushClient{queryNode: map[string]actions.Node{"a.txt": {FileID: "srv1", Size: 5}}}
	e2, _, _ := outboundEngine(t, c2, "Photos", "root1")
	node2, err := e2.probeUploadedNode(context.Background(), op, "aa11", 5, true)
	if err != nil {
		t.Fatalf("probeUploadedNode (positive size): %v", err)
	}
	if node2.FileID != "srv1" {
		t.Errorf("probe node id = %q, want srv1 adopted on identity+positive size", node2.FileID)
	}
}

// TestOutboundUploadHashLessProbeRecoversLostReply is the end-to-end red test for the third
// site: a modify whose commit landed server-side but whose reply was lost, re-probed on a fresh
// lease against a DSM that reports no hash. Pre-fix the probe could prove nothing, so the op
// funnelled into a spurious keep-both copy of content the server already held; now the id+size
// evidence recovers it as the success it was.
func TestOutboundUploadHashLessProbeRecoversLostReply(t *testing.T) {
	content := []byte("hello")
	wantHash := md4.ContentHash(content)
	uploadConn := &pushClient{
		uploadErr: actions.ErrPathError,
		// The pre-turn divergence probe (finding H1) runs on this connection while it is still
		// healthy and sees the committed node - same id, no hash to compare - so it degrades to
		// identity and lets the push proceed. The poisoning it must not be confused with only
		// starts after the Upload, which is what the recovery probe must not reuse.
		queryNode:        map[string]actions.Node{"a.txt": {FileID: "existf", MTime: 55, Size: int64(len(content))}},
		queryNodeOKFirst: 1,
		queryNodeErr:     actions.ErrConnBroken, // the poisoned upload connection cannot probe
	}
	probeConn := &pushClient{
		// The committed node, reported WITHOUT a content hash.
		queryNode: map[string]actions.Node{"a.txt": {FileID: "existf", MTime: 55, Size: int64(len(content))}},
	}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	tr := &leaseSeqTransport{clients: []syncClient{uploadConn, probeConn}}
	e := New(st, tr, root, "sess", nil)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "existf", FileType: "file", LocalHash: wantHash, LocalSize: int64(len(content)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "existf", LocalHash: wantHash, LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "existf" || b.LocalHash != wantHash {
		t.Errorf("baseline = %+v, want synced existf recovered on id+size evidence", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("op not acked after hash-less recovery: %d remain", n)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("view dir entries = %v, want just a.txt (no spurious _Conflict copy)", entries)
	}
}
