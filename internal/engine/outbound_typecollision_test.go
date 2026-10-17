package engine

// These tests pin the create/create type-collision fix: a file<->dir create/create collision in
// steady-state had no terminal exit on the outbound side. A local directory and a server file
// created concurrently at one path (or the mirror: a local file vs a server directory) made the
// outbound OpMkdir/OpUpload fail ErrPathError forever - the lost-reply recovery probe rightly
// refuses to adopt a node of the opposite type, parentGoneConfirmed reads the (alive) parent
// present, and nothing else in applyOutbound's matrix matches - so the op retried invisibly
// every drain, wedging its conflict chain, while cold start funnels this exact divergence via
// planColdConflict. The fix: a probe that positively finds a server node of the OPPOSITE kind
// with a non-empty id surfaces errServerTypeCollision, which applyOutbound funnels terminally
// into the keep-both resolver (funnelTypeCollision, the 24579-branch mirror). Only positive
// proof escalates: an id-less or failing probe keeps the op queued (transient), pinned by the
// Inconclusive regression below.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// findConflictEntries returns the names under dir that carry the keep-both _Conflict marker.
func findConflictEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", dir, err)
	}
	var names []string
	for _, en := range entries {
		if strings.Contains(en.Name(), "_Conflict") {
			names = append(names, en.Name())
		}
	}
	return names
}

// TestOutboundMkdirTypeCollisionFunnels drives the local-dir-vs-server-file half: the mkdir's
// ErrPathError probe finds a regular FILE (non-empty id) at the path, so the op funnels into
// the keep-both resolver instead of retrying forever, and - within the same drain, since
// drainQueue pages until the queue is empty - the resolver completes the full keep-both: the
// server file wins on the canonical path, the local directory (contents and all) is set aside
// whole as a _Conflict copy for the next scan to re-upload, and a durable Conflict record is
// written. A child upload queued behind the mkdir resolves terminally too (errParentConflicted
// - the funnel flips the mkdir's pending baseline StateConflict FIRST, exactly so the child
// cannot wedge on an id-less pending parent whose op left the queue). Before the fix the drain
// errored and left both ops queued, permanently (verified by stashing the fix).
func TestOutboundMkdirTypeCollisionFunnels(t *testing.T) {
	srvBody := []byte("server-file-content")
	srvHash := md4.ContentHash(srvBody)
	c := &pushClient{
		mkdirErr: actions.ErrPathError,
		queryNode: map[string]actions.Node{
			"docs": {FileID: "srvfile", IsDir: false, Hash: srvHash, Size: int64(len(srvBody)), MTime: 777},
		},
		downloadContent: map[string][]byte{"srvfile": srvBody},
		downloadNode: map[string]actions.Node{
			"srvfile": {FileID: "srvfile", Hash: srvHash, Size: int64(len(srvBody)), MTime: 777},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 10, 30, 4, 0, time.UTC))

	// The local directory the user created, with a child file the set-aside must carry along.
	if err := os.MkdirAll(filepath.Join(viewDir, "docs"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "docs", "inner.txt"), []byte("inner"), 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if err := st.PutBaseline(7, "docs", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if err := st.PutBaseline(7, "docs/inner.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash([]byte("inner")), LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "docs", LocalMTime: 100,
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "docs/inner.txt",
		LocalHash: md4.ContentHash([]byte("inner")), LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue child upload: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue = %d ops, want empty (pre-fix: both ops wedged forever)", n)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, "docs"))
	if rerr != nil {
		t.Fatalf("canonical path is not a readable file: %v", rerr)
	}
	if string(got) != string(srvBody) {
		t.Errorf("canonical content = %q, want server bytes %q", got, srvBody)
	}
	copies := findConflictEntries(t, viewDir)
	if len(copies) != 1 {
		t.Fatalf("_Conflict entries = %v, want exactly one set-aside directory", copies)
	}
	inner, rerr := os.ReadFile(filepath.Join(viewDir, copies[0], "inner.txt"))
	if rerr != nil || string(inner) != "inner" {
		t.Errorf("set-aside child = %q (err %v), want the local dir carried whole", inner, rerr)
	}
	b, gerr := st.GetBaseline(7, "docs")
	if gerr != nil {
		t.Fatalf("GetBaseline: %v", gerr)
	}
	if b.State != store.StateSynced || b.FileID != "srvfile" || b.FileType != "file" {
		t.Errorf("resolved baseline = %+v, want synced file srvfile", b)
	}
	conflicts, cerr := st.ListConflicts(7)
	if cerr != nil || len(conflicts) == 0 {
		t.Errorf("ListConflicts = %v (err %v), want a durable keep-both record", conflicts, cerr)
	}
	// The child upload terminally dead-lettered via errParentConflicted (visible as Dropped)
	// instead of wedging on errParentUnknown behind the funnelled mkdir.
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 (the child upload under the conflicted mkdir)", n)
	}
}

// TestOutboundUploadServerDirAppearsAfterTheGuard pins the POST-turn arm of the same funnel
// (outbound_upload.go's serverDirOccupiesPath probe), which the H1 pre-turn guard would
// otherwise have made dead code: guardUploadTarget answers first now, so the sibling test
// below no longer reaches Upload at all and its uploadErr scripting is inert.
//
// The arm is not redundant - it owns the window the pre-turn probe cannot see: a directory a
// peer creates BETWEEN the probe and the push. Modelled with queryNodeHook, whose per-call
// answer is the only way to express a server whose reply for one path CHANGES inside one
// apply: the guard's probe reports the path free (the ordinary answer for a create), the
// Upload then fails ErrPathError, and every probe after it finds the peer's directory.
func TestOutboundUploadServerDirAppearsAfterTheGuard(t *testing.T) {
	c := &pushClient{uploadErr: actions.ErrPathError}
	c.queryNodeHook = func(p string, call int) (actions.Node, error, bool) {
		if p != "notes.txt" {
			return actions.Node{}, nil, false
		}
		if call == 1 {
			// The pre-turn guard: the path is free, so the create proceeds.
			return actions.Node{}, actions.ErrPathError, true
		}
		// The peer's directory landed while the body was being written.
		return actions.Node{FileID: "srvdir", IsDir: true}, nil, true
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 10, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("local"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash([]byte("local")), LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "notes.txt",
		LocalHash: md4.ContentHash([]byte("local")), LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The push really was attempted - otherwise this test would be the sibling below again,
	// exercising the pre-turn guard and leaving the post-turn arm dead.
	if len(c.uploads) == 0 {
		t.Fatal("Upload was never called; the pre-turn guard short-circuited and the post-turn arm is untested")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue = %d ops, want empty (the collision must terminate, not retry forever)", n)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 (the unresolvable server-dir collision is a visible drop)", n)
	}
	b, gerr := st.GetBaseline(7, "notes.txt")
	if gerr != nil {
		t.Fatalf("GetBaseline: %v", gerr)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict (divergence stays surfaced)", b.State)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, "notes.txt"))
	if rerr != nil || string(got) != "local" {
		t.Errorf("local file = %q (err %v), want untouched %q", got, rerr, "local")
	}
}

// TestOutboundUploadServerDirCollisionFunnels drives the mirror half: a local file whose
// upload finds a server DIRECTORY at the same path funnels into the resolver, whose
// errConflictServerDir doctrine dead-letters it observably (StateConflict baseline + Dropped
// trace) - the local file stays intact on disk. Before the fix the upload retried forever
// (verified by stashing the fix). Since finding H1 the verdict is reached PRE-turn, by
// guardUploadTarget's is_dir arm, so no Upload is issued at all; the post-turn arm this test
// used to drive is covered by the sibling above.
func TestOutboundUploadServerDirCollisionFunnels(t *testing.T) {
	c := &pushClient{
		uploadErr: actions.ErrPathError,
		queryNode: map[string]actions.Node{
			"notes.txt": {FileID: "srvdir", IsDir: true},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, "laptop", time.Date(2026, 7, 17, 10, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("local"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "notes.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash([]byte("local")), LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "notes.txt",
		LocalHash: md4.ContentHash([]byte("local")), LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("queue = %d ops, want empty (pre-fix: the upload wedged forever)", n)
	}
	if n, _ := st.DeadLetterLen(); n != 1 {
		t.Errorf("DeadLetterLen = %d, want 1 (the unresolvable server-dir collision is a visible drop)", n)
	}
	b, gerr := st.GetBaseline(7, "notes.txt")
	if gerr != nil {
		t.Fatalf("GetBaseline: %v", gerr)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want StateConflict (divergence stays surfaced)", b.State)
	}
	got, rerr := os.ReadFile(filepath.Join(viewDir, "notes.txt"))
	if rerr != nil || string(got) != "local" {
		t.Errorf("local file = %q (err %v), want untouched %q", got, rerr, "local")
	}
}

// TestOutboundCreateTypeCollisionInconclusiveStaysQueued pins the conservative side: only a
// POSITIVE opposite-type probe (non-empty id) escalates. A probe that finds an id-less node,
// or nothing at all, keeps the op queued as the plain transient it always was - a flaky DSM
// reply must never be escalated into a terminal keep-both funnel.
func TestOutboundCreateTypeCollisionInconclusiveStaysQueued(t *testing.T) {
	t.Run("mkdir/idless-file", func(t *testing.T) {
		c := &pushClient{
			mkdirErr: actions.ErrPathError,
			// A file node WITHOUT an id: not positive proof of a collision.
			queryNode: map[string]actions.Node{"docs": {IsDir: false}},
		}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		if err := os.MkdirAll(filepath.Join(viewDir, "docs"), 0o755); err != nil {
			t.Fatalf("seed dir: %v", err)
		}
		if err := st.PutBaseline(7, "docs", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
			t.Fatalf("seed baseline: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "docs",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatal("drainQueue succeeded, want the inconclusive probe to surface a transient error")
		}
		ops, err := st.PendingOps()
		if err != nil {
			t.Fatalf("PendingOps: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpMkdir {
			t.Fatalf("queue = %+v, want the mkdir still queued", ops)
		}
	})

	t.Run("upload/absent-node", func(t *testing.T) {
		c := &pushClient{
			uploadErr: actions.ErrPathError,
			// QueryNode resolves to the zero node (no id, not a dir): inconclusive.
			queryNode: map[string]actions.Node{},
		}
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		if err := os.WriteFile(filepath.Join(viewDir, "notes.txt"), []byte("local"), 0o644); err != nil {
			t.Fatalf("seed file: %v", err)
		}
		if err := st.PutBaseline(7, "notes.txt", store.Baseline{
			FileType: "file", State: store.StatePending,
		}); err != nil {
			t.Fatalf("seed baseline: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "notes.txt",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatal("drainQueue succeeded, want the inconclusive probe to surface a transient error")
		}
		ops, err := st.PendingOps()
		if err != nil {
			t.Fatalf("PendingOps: %v", err)
		}
		if len(ops) != 1 || ops[0].Type != store.OpUpload {
			t.Fatalf("queue = %+v, want the upload still queued", ops)
		}
	})
}
