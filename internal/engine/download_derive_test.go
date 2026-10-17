package engine

// These tests cover hash/size derivation: a download reply that tolerantly omits the content
// hash (and/or size) must have those facts derived from the SEALED STAGING TEMP - the bytes
// that actually streamed - never by re-reading the promoted path after the rename. Re-reading
// the live path swallows a local edit that lands in the promote→record window into the
// baseline as server-agreed content: unlike an ordinary post-sync edit, such an edit agrees
// with its own baseline forever, so it is never uploaded and both ends report synced while
// the server keeps the older content.

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// editInPromoteWindow swaps e's directory-flush seam so that the first flush of a promoted download's
// parent directory - which happens immediately AFTER the rename landed the file and before the
// caller records its baseline - writes content over target. That is exactly the window an
// editor auto-save occupies on a file that just appeared, and the only way to reach it from a
// test: everything else in the promote sequence is a syscall on the temp.
func editInPromoteWindow(t *testing.T, e *Engine, target string, content []byte) {
	t.Helper()
	orig := priorSyncDir(e)
	var once sync.Once
	e.syncDir = func(dir string) error {
		if err := orig(dir); err != nil {
			return err
		}
		if _, err := os.Lstat(target); err != nil {
			return nil
		}
		once.Do(func() {
			if werr := os.WriteFile(target, content, 0o644); werr != nil {
				t.Errorf("inject promote-window edit at %q: %v", target, werr)
			}
		})
		return nil
	}
}

// TestDownloadHashLessReplyDerivesFactsFromStagedTemp is the red test for the inbound half: a
// hash-less, size-less download reply plus an edit landing in the promote→record window. The
// baseline must describe the STREAMED bytes, so the next scan still sees the local edit as a
// divergence and pushes it. Before the fix the post-promote re-hash recorded the edit itself as
// both LocalHash and ServerHash, and the scan saw agreement forever.
func TestDownloadHashLessReplyDerivesFactsFromStagedTemp(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	serverBytes := []byte("bytes that streamed from the server")
	editBytes := []byte("an editor auto-save that landed right after the promote")
	// A tolerantly-decoded reply: no hash (Download warned and skipped verification) and no size.
	dl := &dlClient{
		content: map[string][]byte{"f1": serverBytes},
		nodes:   map[string]actions.Node{"f1": {FileID: "f1", MTime: 1700000000}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	abs := filepath.Join(root, "Photos", "a.txt")
	editInPromoteWindow(t, e, abs, editBytes)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The window edit really did land (the seam fired) - otherwise the test proves nothing.
	got, err := os.ReadFile(abs)
	if err != nil {
		t.Fatalf("read promoted file: %v", err)
	}
	if string(got) != string(editBytes) {
		t.Fatalf("promoted file = %q, want the injected window edit %q", got, editBytes)
	}

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	wantHash := md4.ContentHash(serverBytes)
	if b.LocalHash != wantHash || b.ServerHash != wantHash {
		t.Errorf("baseline hashes = %q/%q, want the streamed bytes' hash %q (the window edit was swallowed)",
			b.LocalHash, b.ServerHash, wantHash)
	}
	if b.LocalSize != int64(len(serverBytes)) {
		t.Errorf("baseline size = %d, want the streamed size %d", b.LocalSize, len(serverBytes))
	}

	// The consequence that matters: the next scan sees the edit as a divergence and pushes it.
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "a.txt" {
		t.Fatalf("next scan emitted %+v, want a single OpUpload for a.txt (the edit must not be lost)", ops)
	}
	if ops[0].LocalHash != md4.ContentHash(editBytes) {
		t.Errorf("upload op hash = %q, want the edit's hash %q", ops[0].LocalHash, md4.ContentHash(editBytes))
	}
}

// TestDownloadHashCarryingReplyKeepsReplyFacts pins that the derive is skipped entirely when
// the reply carries its facts: Download already verified them against the streamed bytes, so
// the recorded baseline is the reply's hash/size verbatim - a window edit changes nothing.
func TestDownloadHashCarryingReplyKeepsReplyFacts(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	serverBytes := []byte("hello world")
	dl := &dlClient{
		content: map[string][]byte{"f1": serverBytes},
		nodes: map[string]actions.Node{"f1": {
			FileID: "f1", Hash: md4.ContentHash(serverBytes), MTime: 1700000000, Size: int64(len(serverBytes)),
		}},
	}
	e := New(st, &concTransport{client: dl}, root, "sess", nil)
	abs := filepath.Join(root, "Photos", "a.txt")
	editInPromoteWindow(t, e, abs, []byte("a much longer concurrent edit than the server bytes"))

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "a.txt", FileID: "f1", UnixPerm: 644,
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
	if want := md4.ContentHash(serverBytes); b.LocalHash != want || b.ServerHash != want {
		t.Errorf("baseline hashes = %q/%q, want the reply's hash %q", b.LocalHash, b.ServerHash, want)
	}
	if b.LocalSize != int64(len(serverBytes)) {
		t.Errorf("baseline size = %d, want the reply's size %d", b.LocalSize, len(serverBytes))
	}
}

// TestResolveConflictHashLessDownloadDerivesFactsFromStagedTemp is the keep-both mirror: the
// canonical download's reply omits hash and size, and an edit lands on the canonical path in
// the promote→record window. The resolution's whole promise is that content on that path is
// preserved, so the recorded baseline must describe the streamed bytes and leave the edit
// visible to the next scan - not absorb it as server-agreed content.
func TestResolveConflictHashLessDownloadDerivesFactsFromStagedTemp(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	editBytes := []byte("an edit that landed on the canonical path mid-resolution")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		// The download reply itself carries neither hash nor size (tolerant decode).
		downloadNode: map[string]actions.Node{"srv1": {FileID: "srv1", MTime: 555}},
		uploadNode:   map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)
	editInPromoteWindow(t, e, filepath.Join(viewDir, "a.txt"), editBytes)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The window edit landed on the canonical path.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(editBytes) {
		t.Fatalf("canonical content = %q (err %v), want the injected window edit %q", got, err, editBytes)
	}
	// The divergent local content is still preserved as the keep-both copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	cb, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.ServerHash != serverHash || cb.LocalHash != serverHash {
		t.Errorf("canonical baseline hashes = %q/%q, want the streamed bytes' hash %q (the window edit was swallowed)",
			cb.ServerHash, cb.LocalHash, serverHash)
	}
	if cb.LocalSize != int64(len(serverBytes)) {
		t.Errorf("canonical baseline size = %d, want the streamed size %d", cb.LocalSize, len(serverBytes))
	}
}

// TestDeriveStagedDownloadFacts pins the helper's arms directly: a fact the reply already
// carries is never overwritten, an omitted one is read from the temp's own descriptor, and an
// empty file yields the MD4 of no bytes rather than an empty hash.
func TestDeriveStagedDownloadFacts(t *testing.T) {
	content := []byte("staged bytes")
	tests := []struct {
		name     string
		body     []byte
		node     actions.Node
		wantHash string
		wantSize int64
	}{
		{
			name:     "derives both",
			body:     content,
			node:     actions.Node{FileID: "f1"},
			wantHash: md4.ContentHash(content),
			wantSize: int64(len(content)),
		},
		{
			name:     "keeps reply facts",
			body:     content,
			node:     actions.Node{FileID: "f1", Hash: "h-from-reply", Size: 99},
			wantHash: "h-from-reply",
			wantSize: 99,
		},
		{
			name:     "derives hash only",
			body:     content,
			node:     actions.Node{FileID: "f1", Size: 99},
			wantHash: md4.ContentHash(content),
			wantSize: 99,
		},
		{
			name:     "empty body hashes to the empty-content MD4",
			body:     nil,
			node:     actions.Node{FileID: "f1"},
			wantHash: emptyContentMD4,
			wantSize: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp, err := os.CreateTemp(t.TempDir(), "staged")
			if err != nil {
				t.Fatalf("CreateTemp: %v", err)
			}
			defer tmp.Close()
			if _, err := tmp.Write(tt.body); err != nil {
				t.Fatalf("write staged body: %v", err)
			}
			node := tt.node
			if err := deriveStagedDownloadFacts(tmp, &node); err != nil {
				t.Fatalf("deriveStagedDownloadFacts: %v", err)
			}
			if node.Hash != tt.wantHash {
				t.Errorf("hash = %q, want %q", node.Hash, tt.wantHash)
			}
			if node.Size != tt.wantSize {
				t.Errorf("size = %d, want %d", node.Size, tt.wantSize)
			}
		})
	}
}

// TestDeriveStagedDownloadFactsClosedTempErrors pins that a temp the derive cannot read fails
// the fill (and so aborts the promote) instead of recording an empty hash.
func TestDeriveStagedDownloadFactsClosedTempErrors(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "staged")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if _, err := tmp.Write([]byte("body")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	node := actions.Node{FileID: "f1"}
	if err := deriveStagedDownloadFacts(tmp, &node); err == nil {
		t.Fatal("deriveStagedDownloadFacts on a closed temp succeeded, want an error")
	}
	if node.Hash != "" {
		t.Errorf("hash = %q, want it left empty on failure", node.Hash)
	}
}
