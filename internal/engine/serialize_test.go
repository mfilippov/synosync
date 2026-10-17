package engine

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// serialize_test.go locks the serialisation invariant (DESIGN §6, §13): two ops
// conflict iff their resolved paths are equal / one contains the other. The
// properties below check the helpers in serialize.go (cleanKey, keysConflict,
// pathsOverlap, isAncestor, serializeKeys) against an independent, segment-based
// reference so a regression - e.g. a char-prefix ancestor test that mistook "a/b"
// for an ancestor of "a/bc" - is caught rather than the implementation merely
// agreeing with itself. Inputs are generated with testing/quick over a fixed seed
// so failures reproduce.

// quickConfig is the shared testing/quick configuration for the PURE key-level
// properties below: a fixed-seed PRNG (so any counterexample reproduces) and a
// generous case count. Those properties compute conflict keys in memory and touch
// neither disk nor store, so 2000 cases cost microseconds.
func quickConfig() *quick.Config {
	return &quick.Config{
		MaxCount: 2000,
		Rand:     rand.New(rand.NewSource(1)),
	}
}

// schedQuickConfig is the configuration for the two scheduler-level properties at
// the bottom of this file, which are NOT pure: every generated case opens a real
// bbolt store and drives real inbound applies, so each case pays a store commit per
// op plus the durable directory flushes fsutil.Mkdir/Rename/promoteStagedDownload
// perform. Those flushes are real syscalls on Windows too
// (fsyncDir was a no-op there before, on a premise that proved false), and the
// replace-promote goes through MOVEFILE_WRITE_THROUGH - which took these two cases
// to 228s of the engine package's 281s under -race on a native Windows run (every
// other test in the package is under 3s), leaving thin headroom under `go test`'s
// default per-package timeout on the slowest CI platform.
//
// The count is sized for the property rather than for the generator: a single case
// already drives up to 11 ops over 6 overlapping paths through the real concurrent
// scheduler, so every case samples many interleavings, and 200 of them still drive
// ~1100 ops per property. Same fixed seed, so any counterexample reproduces - and
// because the seed and the generators are unchanged, these cases are a prefix of
// the sequence the larger count used to walk. Redness pinned by re-running both
// properties at this count against a scheduler whose serializeKeys returns a
// per-op-unique key: each fails on its first conflicting sequence.
//
// Deliberately NOT solved by stubbing the directory-flush seam: these two are the only end-to-end
// coverage of the real apply path under concurrency, a stub would quietly exempt
// exactly the durability discipline the Windows flush restored, and it would not even be
// sufficient - most of the flushes an apply pays are inside fsutil.Mkdir/Rename,
// which do not route through the engine's seam.
func schedQuickConfig() *quick.Config {
	return &quick.Config{
		MaxCount: 200,
		Rand:     rand.New(rand.NewSource(1)),
	}
}

// segTokens is a small alphabet whose entries share prefixes ("a"/"ab"/"abc",
// "b"/"bc") so the generator routinely produces neighbouring names with a common
// character prefix - exactly the case that distinguishes a segment-boundary
// ancestor test from a naive byte-prefix one.
var segTokens = []string{"a", "ab", "abc", "b", "bc", "c"}

// segPath is a generated view-relative slash path of segTokens segments (possibly
// empty, i.e. the view root). It implements quick.Generator.
type segPath string

// Generate builds a random slash path of 0..4 tokens drawn from segTokens.
func (segPath) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(5)
	segs := make([]string, n)
	for i := range segs {
		segs[i] = segTokens[r.Intn(len(segTokens))]
	}
	return reflect.ValueOf(segPath(strings.Join(segs, "/")))
}

// keySegs splits an anchored, cleaned conflict key (as cleanKey produces) into its
// non-empty path segments - the independent representation the reference predicates
// operate on.
func keySegs(key string) []string {
	parts := strings.Split(key, string(filepath.Separator))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// segsPrefix reports whether a is a (non-strict) prefix of b, element by element.
func segsPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// refOverlap is the reference for pathsOverlap: two paths overlap iff one's segment
// list is a (non-strict) prefix of the other's.
func refOverlap(ka, kb string) bool {
	sa, sb := keySegs(ka), keySegs(kb)
	return segsPrefix(sa, sb) || segsPrefix(sb, sa)
}

// refAncestor is the reference for isAncestor: anc is a strict ancestor of desc iff
// anc's segment list is a strict prefix of desc's.
func refAncestor(anc, desc string) bool {
	sa, sd := keySegs(anc), keySegs(desc)
	return len(sa) < len(sd) && segsPrefix(sa, sd)
}

// TestPathsOverlapMatchesSegmentReference locks pathsOverlap to the segment-prefix
// definition of "equal / contains / contained" for paths under one viewDir.
func TestPathsOverlapMatchesSegmentReference(t *testing.T) {
	const viewDir = "view-1"
	prop := func(a, b segPath) bool {
		ka := cleanKey(viewDir, string(a))
		kb := cleanKey(viewDir, string(b))
		return pathsOverlap(ka, kb) == refOverlap(ka, kb)
	}
	if err := quick.Check(prop, quickConfig()); err != nil {
		t.Fatal(err)
	}
}

// TestPathsOverlapMatchesDefinition pins pathsOverlap to its own stated definition
// (equal || isAncestor either way) so the three predicates stay consistent.
func TestPathsOverlapMatchesDefinition(t *testing.T) {
	const viewDir = "view-1"
	prop := func(a, b segPath) bool {
		ka := cleanKey(viewDir, string(a))
		kb := cleanKey(viewDir, string(b))
		want := ka == kb || isAncestor(ka, kb) || isAncestor(kb, ka)
		return pathsOverlap(ka, kb) == want
	}
	if err := quick.Check(prop, quickConfig()); err != nil {
		t.Fatal(err)
	}
}

// TestIsAncestorRespectsSegmentBoundary locks isAncestor to the strict-segment-prefix
// reference: a shared character prefix that is not a segment boundary ("a/b" vs
// "a/bc") must not register as an ancestor.
func TestIsAncestorRespectsSegmentBoundary(t *testing.T) {
	const viewDir = "view-1"
	prop := func(anc, desc segPath) bool {
		ka := cleanKey(viewDir, string(anc))
		kd := cleanKey(viewDir, string(desc))
		return isAncestor(ka, kd) == refAncestor(ka, kd)
	}
	if err := quick.Check(prop, quickConfig()); err != nil {
		t.Fatal(err)
	}

	// Explicit boundary cases: a byte-prefix that is not a segment prefix.
	cases := []struct {
		anc, desc string
		want      bool
	}{
		{"a/b", "a/bc", false},
		{"a/b", "a/b/c", true},
		{"a/b", "a/b", false}, // equal is not a strict ancestor
		{"ab", "abc", false},
		{"a", "a/b", true},
	}
	for _, c := range cases {
		ka := cleanKey("v", c.anc)
		kd := cleanKey("v", c.desc)
		if got := isAncestor(ka, kd); got != c.want {
			t.Errorf("isAncestor(%q, %q) = %v, want %v", c.anc, c.desc, got, c.want)
		}
	}

	// The synthetic root - a lone separator - is a strict ancestor of every other
	// path but not of itself: the anc==sep branch of isAncestor, which a non-empty
	// viewDir never reaches in production.
	sep := string(filepath.Separator)
	if !isAncestor(sep, cleanKey("v", "a")) {
		t.Errorf("isAncestor(root, %q) = false, want true", cleanKey("v", "a"))
	}
	if isAncestor(sep, sep) {
		t.Error("isAncestor(root, root) = true, want false (root is not its own ancestor)")
	}

	// cleanKey("", "") is the degenerate empty-viewDir input that reaches that
	// branch. Its output is Clean(sep+sep), which is the lone separator only where
	// Clean collapses a doubled separator: on Windows a leading `\\` is a UNC
	// volume prefix, so the root there is `\\` and isAncestor takes the
	// HasPrefix branch instead. Assert the actual per-OS production output rather
	// than the POSIX literal - the empty-viewDir case is unreachable anyway
	// (viewDir is the view's on-disk name, never empty).
	wantRoot := filepath.Clean(sep + sep)
	if root := cleanKey("", ""); root != wantRoot {
		t.Fatalf("cleanKey(\"\", \"\") = %q, want root %q", root, wantRoot)
	}
}

// TestKeysConflictSymmetricAndReflexive checks the two scheduler-facing algebraic
// properties of keysConflict: it is symmetric, and every non-empty key set conflicts
// with itself (so an op always serialises against a copy of itself).
func TestKeysConflictSymmetricAndReflexive(t *testing.T) {
	const viewDir = "view-1"
	keysOf := func(p segPath) []string { return []string{cleanKey(viewDir, string(p))} }

	sym := func(a, b segPath) bool {
		ka, kb := keysOf(a), keysOf(b)
		return keysConflict(ka, kb) == keysConflict(kb, ka)
	}
	if err := quick.Check(sym, quickConfig()); err != nil {
		t.Fatalf("symmetry: %v", err)
	}

	refl := func(a segPath) bool {
		ka := keysOf(a)
		return len(ka) > 0 && keysConflict(ka, ka)
	}
	if err := quick.Check(refl, quickConfig()); err != nil {
		t.Fatalf("reflexivity: %v", err)
	}
}

// TestSerializeKeysShape locks serializeKeys: a rename with distinct endpoints yields
// both endpoint keys, every other op (and a no-op rename) yields exactly one, and the
// keys are the cleanKey resolution of the op's paths.
func TestSerializeKeysShape(t *testing.T) {
	const viewDir = "view-1"

	renameTwo := func(p, np segPath) bool {
		if string(p) == string(np) {
			return true // covered by renameSame below
		}
		op := store.Op{Type: store.OpRename, Path: string(p), NewPath: string(np)}
		keys := serializeKeys(viewDir, op)
		// Distinct endpoints can still clean to the same key (folding/normalisation);
		// then serializeKeys keeps both raw keys but they are equal - accept either a
		// 2-key set or a collapsed 1-key set whose member is the shared key.
		ksrc := cleanKey(viewDir, string(p))
		kdst := cleanKey(viewDir, string(np))
		if ksrc == kdst {
			return len(keys) == 2 && keys[0] == ksrc && keys[1] == kdst
		}
		return len(keys) == 2 &&
			((keys[0] == ksrc && keys[1] == kdst) || (keys[0] == kdst && keys[1] == ksrc))
	}
	if err := quick.Check(renameTwo, quickConfig()); err != nil {
		t.Fatalf("rename two endpoints: %v", err)
	}

	renameSame := func(p segPath) bool {
		op := store.Op{Type: store.OpRename, Path: string(p), NewPath: string(p)}
		keys := serializeKeys(viewDir, op)
		return len(keys) == 1 && keys[0] == cleanKey(viewDir, string(p))
	}
	if err := quick.Check(renameSame, quickConfig()); err != nil {
		t.Fatalf("rename same endpoint: %v", err)
	}

	single := func(p segPath) bool {
		ok := true
		for _, ty := range []store.OpType{store.OpDownload, store.OpMkdir, store.OpRemove, store.OpUpload} {
			op := store.Op{Type: ty, Path: string(p)}
			keys := serializeKeys(viewDir, op)
			if len(keys) != 1 || keys[0] != cleanKey(viewDir, string(p)) {
				ok = false
			}
		}
		return ok
	}
	if err := quick.Check(single, quickConfig()); err != nil {
		t.Fatalf("single-key ops: %v", err)
	}
}

// TestSerializeKeysDistinctViewDirsDisjoint checks that the same view-relative path in
// two non-nested views resolves to non-conflicting keys, so ops in different views do
// not serialise against each other.
func TestSerializeKeysDistinctViewDirsDisjoint(t *testing.T) {
	prop := func(p segPath) bool {
		op := store.Op{Type: store.OpDownload, Path: string(p)}
		ka := serializeKeys("alpha", op)
		kb := serializeKeys("beta", op)
		return !keysConflict(ka, kb)
	}
	if err := quick.Check(prop, quickConfig()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Scheduler-level property tests (DESIGN §6): the drainBatch scheduler must
// honour the serialisation invariant end-to-end - two ops whose conflict keys
// overlap (keysConflict) never run concurrently and always apply in FIFO
// (enqueue / ascending-sync_id) order, while independent paths proceed in
// parallel. The key-level helpers are pinned above; these drive the real
// concurrent scheduler over randomly generated op sequences and observe the
// applied order through an instrumented download client. Run under -race
// (make test), so an actual data race between conflicting ops would also fail.
// ---------------------------------------------------------------------------

// dlSchedPaths is the pool of view-relative download paths the scheduler property
// draws from. No entry is a path-segment prefix of another (so two distinct paths
// are always independent and may run concurrently), while repeats of the same path
// produce equal-key conflicts that must serialise. "sub/x.txt" and "sub/y.txt" share
// a parent directory yet neither contains the other, so they stay independent.
var dlSchedPaths = []string{
	"a.txt", "b.txt", "c.txt", "sub/x.txt", "sub/y.txt", "deep/m/n.txt",
}

// dlOpSeq is a generated sequence of download ops, each element an index into
// dlSchedPaths. It implements quick.Generator.
type dlOpSeq []int

// Generate builds a random run of 0..11 download ops over dlSchedPaths.
func (dlOpSeq) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(12)
	s := make(dlOpSeq, n)
	for i := range s {
		s[i] = r.Intn(len(dlSchedPaths))
	}
	return reflect.ValueOf(s)
}

// trackingClient is a concurrency-instrumented syncClient. Its Download records, per
// file id, the conflict key the op resolved to, detects whether any conflicting op is
// already active when it enters (a serialisation violation), tracks the maximum number
// of concurrently active downloads (to confirm independent paths really parallelise)
// and records the order downloads are entered (to confirm conflicting ops enter in FIFO
// order). It is safe for concurrent use from the scheduler's per-op goroutines.
type trackingClient struct {
	mu       sync.Mutex
	keyOf    map[string]string // file id -> resolved conflict key
	idx      map[string]int    // file id -> enqueue index
	content  map[string][]byte // file id -> bytes to write
	node     actions.Node      // facts returned for every download
	active   map[string]string // file id -> key, currently in-flight
	maxAct   int               // high-water concurrent downloads
	enterSeq []string          // file ids in the order Download was entered
	viol     []string          // recorded "conflicting ops overlapped" violations
	delay    time.Duration
}

func (c *trackingClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *trackingClient) QueryNode(context.Context, int64, string) (actions.Node, error) {
	return actions.Node{}, nil
}
func (c *trackingClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *trackingClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}

func (c *trackingClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	key := c.keyOf[fileID]
	for other, okey := range c.active {
		if keysConflict([]string{okey}, []string{key}) {
			c.viol = append(c.viol, fmt.Sprintf("%s and %s ran concurrently on conflicting keys %q/%q", other, fileID, okey, key))
		}
	}
	c.active[fileID] = key
	if len(c.active) > c.maxAct {
		c.maxAct = len(c.active)
	}
	c.enterSeq = append(c.enterSeq, fileID)
	c.mu.Unlock()

	if c.delay > 0 {
		time.Sleep(c.delay)
	}

	c.mu.Lock()
	delete(c.active, fileID)
	c.mu.Unlock()

	if _, err := w.Write(c.content[fileID]); err != nil {
		return actions.Node{}, err
	}
	// Report the real MD4 of the bytes just delivered, as a faithful server does (the
	// production actions.Download verifies exactly that): the synced baseline then records a
	// LocalHash matching the promoted content, so a later same-path download's live-base
	// divergence check sees the prior output as in sync, not as a phantom
	// local edit.
	node := c.node
	node.Hash = md4.ContentHash(c.content[fileID])
	return node, nil
}

// TestSchedulerSerializesConflictingDownloads is the core scheduler-level property:
// over random sequences of inbound downloads across several overlapping paths, no two
// conflicting ops (by keysConflict) ever run concurrently, conflicting ops enter in
// strict FIFO (enqueue) order, the whole queue drains (every op acked), and every
// touched path ends synced with the last-enqueued op's content. Across the whole run it
// also confirms independent paths really do parallelise (max concurrent downloads > 1).
func TestSchedulerSerializesConflictingDownloads(t *testing.T) {
	const viewID = 7
	const viewName = "V"

	globalMax := 0
	prop := func(seq dlOpSeq) bool {
		st := newTestStore(t)
		if err := st.PutView(viewID, store.View{Name: viewName}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		root := t.TempDir()
		tc := &trackingClient{
			keyOf:   make(map[string]string),
			idx:     make(map[string]int),
			content: make(map[string][]byte),
			active:  make(map[string]string),
			node:    actions.Node{Hash: "h", MTime: 1700000000, Size: 4},
			delay:   2 * time.Millisecond,
		}
		e := New(st, &concTransport{client: tc}, root, "sess", nil)

		// last records the file id of the final op enqueued for each path so the
		// expected on-disk content/baseline can be checked after the drain.
		last := make(map[string]string)
		for i, p := range seq {
			path := dlSchedPaths[p]
			fid := fmt.Sprintf("f%d", i)
			tc.keyOf[fid] = cleanKey(viewName, path)
			tc.idx[fid] = i
			tc.content[fid] = []byte(fmt.Sprintf("v%02d", i)) // 3 bytes; size in node is advisory here
			last[path] = fid
			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound,
				ViewID: viewID, Path: path, FileID: fid, UnixPerm: 644,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}

		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}

		// Property: conflicting ops never overlapped in flight.
		if len(tc.viol) > 0 {
			t.Errorf("conflicting ops ran concurrently: %v", tc.viol)
			return false
		}

		// Property: conflicting ops entered in FIFO (enqueue-index) order. Within each
		// conflict key the enter order must be ascending in enqueue index.
		lastIdxForKey := make(map[string]int)
		for _, fid := range tc.enterSeq {
			k := tc.keyOf[fid]
			if prev, ok := lastIdxForKey[k]; ok && tc.idx[fid] < prev {
				t.Errorf("FIFO violation on key %q: enqueue idx %d entered after %d", k, tc.idx[fid], prev)
				return false
			}
			lastIdxForKey[k] = tc.idx[fid]
		}

		// Property: the whole queue drained (every op acked).
		if n, err := st.PendingLen(); err != nil {
			t.Fatalf("PendingLen: %v", err)
		} else if n != 0 {
			t.Errorf("queue not fully drained: %d ops remain", n)
			return false
		}

		// Property: every touched path ends synced with the last-enqueued op's content.
		for path, fid := range last {
			got, err := os.ReadFile(filepath.Join(root, viewName, filepath.FromSlash(path)))
			if err != nil {
				t.Errorf("read %q: %v", path, err)
				return false
			}
			if string(got) != string(tc.content[fid]) {
				t.Errorf("path %q content = %q, want last op %q (%q)", path, got, fid, tc.content[fid])
				return false
			}
			b, err := st.GetBaseline(viewID, path)
			if err != nil {
				t.Errorf("GetBaseline %q: %v", path, err)
				return false
			}
			if b.State != store.StateSynced || b.FileID != fid {
				t.Errorf("baseline %q = {%q,%q}, want synced fileID %q", path, b.State, b.FileID, fid)
				return false
			}
		}

		if tc.maxAct > globalMax {
			globalMax = tc.maxAct
		}
		return true
	}

	if err := quick.Check(prop, schedQuickConfig()); err != nil {
		t.Fatal(err)
	}
	// Independent paths must really parallelise: across the whole run at least one
	// sequence had two non-conflicting downloads in flight at once.
	if globalMax < 2 {
		t.Errorf("max concurrent downloads across the run = %d, want >1 (independent paths must parallelise)", globalMax)
	}
}

// schedTreePath is a generated path into a small ancestor/descendant tree, used to
// drive mixed mkdir/download op runs through the scheduler. It implements
// quick.Generator. Index 0..len(schedDirs)-1 selects a directory (an OpMkdir), the
// rest select a file (an OpDownload) whose parent directories overlap the dirs - so
// the scheduler routinely chains a child download behind an ancestor mkdir.
type schedTreePath int

var (
	schedDirs  = []string{"d1", "d2", "d1/sub"}
	schedFiles = []string{"d1/a.txt", "d1/b.txt", "d1/sub/c.txt", "d2/e.txt", "top.txt"}
)

func (schedTreePath) Generate(r *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(schedTreePath(r.Intn(len(schedDirs) + len(schedFiles))))
}

// schedTreeSeq is a generated run of 0..11 tree ops.
type schedTreeSeq []schedTreePath

func (schedTreeSeq) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(12)
	s := make(schedTreeSeq, n)
	for i := range s {
		s[i] = schedTreePath(r.Intn(len(schedDirs) + len(schedFiles)))
	}
	return reflect.ValueOf(s)
}

// TestSchedulerMixedTreeDrainsToExpectedState locks the "all non-conflicting chains
// complete, are acked, and leave the baseline in the expected state" property over
// random runs of mixed mkdir/download ops across an ancestor/descendant tree (a child
// download conflicts with - and must serialise behind/around - its ancestor dir's
// mkdir). Whatever the interleaving the scheduler picks, after a full drain the queue is
// empty, every created directory and downloaded file is present, and every touched
// baseline is StateSynced - so a conflicting ancestor/descendant chain neither deadlocks
// nor corrupts state.
func TestSchedulerMixedTreeDrainsToExpectedState(t *testing.T) {
	const viewID = 7
	const viewName = "V"

	prop := func(seq schedTreeSeq) bool {
		st := newTestStore(t)
		if err := st.PutView(viewID, store.View{Name: viewName}); err != nil {
			t.Fatalf("PutView: %v", err)
		}
		root := t.TempDir()
		tc := &trackingClient{
			keyOf:   make(map[string]string),
			idx:     make(map[string]int),
			content: make(map[string][]byte),
			active:  make(map[string]string),
			node:    actions.Node{Hash: "h", MTime: 1700000000, Size: 3},
			delay:   time.Millisecond,
		}
		e := New(st, &concTransport{client: tc}, root, "sess", nil)

		touchedDirs := make(map[string]bool)
		lastFile := make(map[string]string) // file path -> last op file id
		for i, sp := range seq {
			n := int(sp)
			if n < len(schedDirs) {
				dir := schedDirs[n]
				touchedDirs[dir] = true
				if _, err := st.Enqueue(store.Op{
					Type: store.OpMkdir, Direction: store.DirInbound,
					ViewID: viewID, Path: dir, FileID: fmt.Sprintf("d%d", i), LocalMTime: 1700000000,
				}); err != nil {
					t.Fatalf("Enqueue mkdir: %v", err)
				}
				continue
			}
			file := schedFiles[n-len(schedDirs)]
			fid := fmt.Sprintf("f%d", i)
			tc.keyOf[fid] = cleanKey(viewName, file)
			tc.idx[fid] = i
			tc.content[fid] = []byte(fmt.Sprintf("c%02d", i))
			lastFile[file] = fid
			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound,
				ViewID: viewID, Path: file, FileID: fid, UnixPerm: 644,
			}); err != nil {
				t.Fatalf("Enqueue download: %v", err)
			}
		}

		if err := e.drainQueue(context.Background()); err != nil {
			t.Fatalf("drainQueue: %v", err)
		}

		// No conflicting ops overlapped (the download tracker would have recorded it).
		if len(tc.viol) > 0 {
			t.Errorf("conflicting downloads overlapped: %v", tc.viol)
			return false
		}

		if n, err := st.PendingLen(); err != nil {
			t.Fatalf("PendingLen: %v", err)
		} else if n != 0 {
			t.Errorf("queue not fully drained: %d ops remain", n)
			return false
		}

		// Every created directory exists and its baseline is synced.
		dirs := make([]string, 0, len(touchedDirs))
		for d := range touchedDirs {
			dirs = append(dirs, d)
		}
		sort.Strings(dirs)
		for _, dir := range dirs {
			fi, err := os.Stat(filepath.Join(root, viewName, filepath.FromSlash(dir)))
			if err != nil || !fi.IsDir() {
				t.Errorf("directory %q missing after drain: err=%v", dir, err)
				return false
			}
			b, err := st.GetBaseline(viewID, dir)
			if err != nil {
				t.Errorf("GetBaseline dir %q: %v", dir, err)
				return false
			}
			if b.State != store.StateSynced {
				t.Errorf("dir baseline %q state = %q, want synced", dir, b.State)
				return false
			}
		}

		// Every downloaded file exists with its last op's content and a synced baseline.
		for file, fid := range lastFile {
			got, err := os.ReadFile(filepath.Join(root, viewName, filepath.FromSlash(file)))
			if err != nil {
				t.Errorf("read file %q: %v", file, err)
				return false
			}
			if string(got) != string(tc.content[fid]) {
				t.Errorf("file %q content = %q, want %q", file, got, tc.content[fid])
				return false
			}
			b, err := st.GetBaseline(viewID, file)
			if err != nil {
				t.Errorf("GetBaseline file %q: %v", file, err)
				return false
			}
			if b.State != store.StateSynced || b.FileID != fid {
				t.Errorf("file baseline %q = {%q,%q}, want synced fileID %q", file, b.State, b.FileID, fid)
				return false
			}
		}
		return true
	}

	if err := quick.Check(prop, schedQuickConfig()); err != nil {
		t.Fatal(err)
	}
}
