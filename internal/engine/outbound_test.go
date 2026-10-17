package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// pushClient is a concurrency-safe syncClient that also implements outboundClient,
// scripting the push actions (Upload/Mkdir/Rename/Remove/resume) and recording every
// call so the outbound executor can be exercised without a network.
type pushClient struct {
	mu sync.Mutex

	// scripted per-name node results for create actions.
	uploadNode map[string]actions.Node
	mkdirNode  map[string]actions.Node

	// scripted Download results keyed by file id: the bytes written to w and the
	// returned node metadata. Unset ids write nothing and return a zero node (the
	// historical no-op behaviour).
	downloadContent map[string][]byte
	downloadNode    map[string]actions.Node

	// downloadHook, if set, runs once on the first Download call - used to model a concurrent
	// local write at the canonical path mid-resolution. downloadHookCall picks a
	// later call to fire it on instead (1-based; 0 keeps the first-call default), so a test can
	// land the write during the SECOND download of a same-path batch.
	downloadHook      func()
	downloadHookFired bool
	downloadHookCall  int
	downloadCalls     int

	// scripted errors.
	uploadErr   error
	mkdirErr    error
	renameErr   error
	removeErr   error
	downloadErr error

	// uploadFailsLeft returns ErrConnBroken for the first N Upload calls, then succeeds,
	// modelling a re-dial-then-retry across drains.
	uploadFailsLeft int

	// uploadConflictLeft returns ErrConflictVersion (stale-base, 24579) for the first N
	// Upload calls, then succeeds. It models the canonical push hitting a diverged remote
	// (which funnels to the keep-both resolver) while a later copy upload still succeeds.
	uploadConflictLeft int

	// recorded calls.
	uploads []uploadCall
	mkdirs  []mkdirCall
	renames [][2]string
	removes []string

	// resume large-upload scripting.
	resumeToken        actions.ResumeToken
	resumeTokenErr     error
	resumeNode         actions.Node
	resumeExpectedSize int64
	resumeChunks       []byte
	resumeReq          actions.ResumeRequest
	// resumeAssembled holds the streamed bytes written at their declared offset, so an
	// overlapping re-send (after a scripted partial commit) lands in the right place - the
	// way a real offset-addressed server stores them. A correct client reproduces the file
	// here exactly; a client whose reader desyncs from the offset writes the wrong bytes.
	resumeAssembled []byte
	// resumeCommitOffsets scripts the committed offset each ResumeUpload reports, popped in
	// order; an empty list means the server commits the whole chunk (offset+len). A value
	// short of offset+len models a partial commit that forces the client to re-seek and
	// re-send from there.
	resumeCommitOffsets []int64
	// resumeNoComplete suppresses the Done flag on the final chunk, modelling a server
	// that streams the whole file but never reports completion.
	resumeNoComplete bool
	// resumeSpec records the whole-file facts the last ResumeUpload advertised. The
	// size/hash used to ride on the token request; the token is view-scoped now, so
	// they live here.
	resumeSpec actions.ResumeUploadSpec

	// resume download scripting (keep-both large-server-node branch): ResumeDownload serves
	// downloadContent[fileID][offset:offset+resumeDLChunkSize], recording the offsets requested
	// and the call count so a test can assert the chunked path was taken (not single-frame
	// Download). resumeDLChunkSize 0 means the whole remainder in one chunk.
	resumeDLChunkSize int64
	resumeDLCalls     int
	resumeDLOffsets   []int64

	// queryNode scripts the postcondition QueryNode the mkdir/rename replay-safety path
	// issues after an ErrPathError; keyed by the queried path. queryNodeErr forces a
	// lookup failure.
	queryNode    map[string]actions.Node
	queryNodeErr error
	// queryNodeErrByPath scripts a per-path QueryNode error (checked before queryNodeErr), so a
	// test can model the server reporting one path gone (ErrPathError) while another resolves.
	queryNodeErrByPath map[string]error

	// listResult scripts the children List returns, keyed by the queried (parent) path;
	// listErr forces a listing failure (an inconclusive probe). Used by the remove
	// postcondition's parent-listing absence confirmation. listErrByPath scripts a per-path
	// List error (checked before listErr), so a test can model one directory deleted
	// server-side (ErrPathError) while its ancestors still list cleanly.
	// listUnevidenced scripts a SUCCESSFUL turn whose reply carried no children array at all
	// (an errno-only reply): List reports listed=false with no error, the third state
	// actions.Client.List surfaces so absence provers can call it inconclusive instead of
	// reading a zero-length slice as proof of absence.
	listResult      map[string][]actions.Node
	listErr         error
	listErrByPath   map[string]error
	listUnevidenced map[string]bool
	listCalls       []string

	// queryNodeCalls records the paths passed to QueryNode so a test can assert a guard
	// dead-lettered the op BEFORE any server turn (e.g. the read-only-view / rejected-path
	// branches that must run before the lease).
	queryNodeCalls []string

	// queryNodeOKFirst suppresses queryNodeErr for the first N calls on this connection, which
	// answer from queryNode instead. The upload path now probes its target BEFORE the push
	// (guardUploadTarget, finding H1) and the lost-reply recovery probes it again AFTER - on a
	// connection a rejected Upload has poisoned. A blanket queryNodeErr can no longer model that
	// poisoning: it would fail the healthy pre-turn probe too. Answering the first call and
	// breaking afterwards keeps the "the recovery must NOT reuse the upload's connection"
	// discrimination the fresh-lease tests rest on.
	queryNodeOKFirst int

	// queryNodeHook, when set, answers a QueryNode call outright: it receives the queried path
	// and the 1-based call count and reports whether it claims the call. The map-keyed fields
	// above are per-path CONSTANTS, so they cannot model a server whose answer for one path
	// CHANGES between two probes in the same apply - which is exactly the shape the outbound
	// directory delete's two probes must survive (a positive pre-turn QueryNode followed by a
	// timed-out corroboration of the same path). It runs with c.mu held, so it must not call
	// back into the client.
	queryNodeHook func(p string, call int) (actions.Node, error, bool)
}

type uploadCall struct {
	parentID string
	name     string
	content  []byte
}

type mkdirCall struct {
	parentID string
	name     string
}

func (c *pushClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }

// renameDestFree scripts the answer a live server gives for a path that holds nothing: a path
// error, not an empty node. An outbound rename probes its destination before the turn
// (guardRenameTarget), and pushClient's default for an unscripted path - a zero Node with a nil
// error - is a reply that NAMES no node, which the guard rightly refuses to read as "free": the
// reply-evidence rule says an answer with no verdict proves nothing. A rename test that expects
// the move to happen therefore has to say its destination is empty, which is worth stating
// explicitly anyway.
func renameDestFree(paths ...string) map[string]error {
	m := make(map[string]error, len(paths))
	for _, p := range paths {
		m[p] = actions.ErrPathError
	}
	return m
}

func (c *pushClient) QueryNode(_ context.Context, _ int64, p string) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryNodeCalls = append(c.queryNodeCalls, p)
	if c.queryNodeHook != nil {
		if n, err, ok := c.queryNodeHook(p, len(c.queryNodeCalls)); ok {
			return n, err
		}
	}
	if err, ok := c.queryNodeErrByPath[p]; ok {
		return actions.Node{}, err
	}
	if c.queryNodeErr != nil && len(c.queryNodeCalls) > c.queryNodeOKFirst {
		return actions.Node{}, c.queryNodeErr
	}
	return c.queryNode[p], nil
}
func (c *pushClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *pushClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}
func (c *pushClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// downloadHook fires exactly once - on the first Download by default, or on the
	// downloadHookCall-th when set - so a test can model a concurrent local write landing at
	// the canonical path during a chosen download.
	c.downloadCalls++
	if c.downloadHook != nil && !c.downloadHookFired &&
		(c.downloadHookCall == 0 || c.downloadCalls == c.downloadHookCall) {
		c.downloadHookFired = true
		c.downloadHook()
	}
	if c.downloadErr != nil {
		return actions.Node{}, c.downloadErr
	}
	if body, ok := c.downloadContent[fileID]; ok {
		if _, err := w.Write(body); err != nil {
			return actions.Node{}, err
		}
	}
	return c.downloadNode[fileID], nil
}

func (c *pushClient) Upload(_ context.Context, parentID, name string, _, _ int64, content []byte) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.uploadFailsLeft > 0 {
		c.uploadFailsLeft--
		return actions.Node{}, actions.ErrConnBroken
	}
	if c.uploadConflictLeft > 0 {
		c.uploadConflictLeft--
		return actions.Node{}, actions.ErrConflictVersion
	}
	c.uploads = append(c.uploads, uploadCall{parentID: parentID, name: name, content: append([]byte(nil), content...)})
	if c.uploadErr != nil {
		return actions.Node{}, c.uploadErr
	}
	return c.uploadNode[name], nil
}

func (c *pushClient) Mkdir(_ context.Context, parentID, name string, _, _ int64) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mkdirs = append(c.mkdirs, mkdirCall{parentID: parentID, name: name})
	if c.mkdirErr != nil {
		return actions.Node{}, c.mkdirErr
	}
	return c.mkdirNode[name], nil
}

func (c *pushClient) Rename(_ context.Context, _, _ int64, fromPath, toPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.renames = append(c.renames, [2]string{fromPath, toPath})
	return c.renameErr
}

// Remove models the verb as MEASURED on a live DSM, which is the whole point of the fake here:
// `remove` of a DIRECTORY is RECURSIVE - it succeeds on a non-empty directory and takes the
// subtree with it (TestIntegration_RemoveDirectorySemantics, two-client finding H7). A fake that
// merely recorded the call was kinder than the server in exactly the way the
// never-kinder-than-the-server testing rule warns about: it let a guard be "verified" against a
// server that refuses to delete a non-empty directory, which this one does not. So a successful
// Remove drops the node - and everything beneath it - from the scripted server tree the
// List/QueryNode fakes answer from.
func (c *pushClient) Remove(_ context.Context, _ int64, p string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removes = append(c.removes, p)
	if c.removeErr != nil {
		return c.removeErr
	}
	c.dropServerSubtreeLocked(p)
	return nil
}

// dropServerSubtreeLocked deletes p and its whole subtree from the scripted server state:
// the child entry in its parent's listing, its own listing, every listing beneath it, and the
// matching queryNode answers. Called with c.mu held.
func (c *pushClient) dropServerSubtreeLocked(p string) {
	parent := path.Dir(p)
	if parent == "." || parent == "" {
		parent = viewRootPath
	}
	if kids, ok := c.listResult[parent]; ok {
		leaf := path.Base(p)
		kept := make([]actions.Node, 0, len(kids))
		for _, ch := range kids {
			if ch.Name != leaf {
				kept = append(kept, ch)
			}
		}
		c.listResult[parent] = kept
	}
	under := p + "/"
	for k := range c.listResult {
		if k == p || strings.HasPrefix(k, under) {
			delete(c.listResult, k)
		}
	}
	for k := range c.queryNode {
		if k == p || strings.HasPrefix(k, under) {
			delete(c.queryNode, k)
		}
	}
}

// List mirrors actions.Client.List's evidence contract: a successful turn reports
// listed=true (the reply carried a file_list, possibly empty) unless the path is in
// listUnevidenced, which models an errno-only reply carrying no file_list at all.
func (c *pushClient) List(_ context.Context, _ int64, path string) ([]actions.Node, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listCalls = append(c.listCalls, path)
	if err, ok := c.listErrByPath[path]; ok {
		return nil, false, err
	}
	if c.listErr != nil {
		return nil, false, c.listErr
	}
	if c.listUnevidenced[path] {
		return nil, false, nil
	}
	return c.listResult[path], true, nil
}

func (c *pushClient) ResumeGetToken(_ context.Context, req actions.ResumeRequest) (actions.ResumeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeReq = req
	if c.resumeTokenErr != nil {
		return actions.ResumeToken{}, c.resumeTokenErr
	}
	return c.resumeToken, nil
}

// ResumeUpload models the whole-file contract: one call carries the entire content,
// which the fake drains from body exactly as the encoder would stream it.
func (c *pushClient) ResumeUpload(_ context.Context, _ string, spec actions.ResumeUploadSpec, body io.Reader) (actions.Node, error) {
	content, err := io.ReadAll(io.LimitReader(body, spec.Size))
	if err != nil {
		return actions.Node{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeExpectedSize = spec.Size
	c.resumeSpec = spec
	c.resumeAssembled = content
	c.resumeChunks = append(c.resumeChunks, content...)
	node := c.resumeNode
	if node.Hash == "" {
		node.Hash = spec.Hash
	}
	if node.Size == 0 {
		node.Size = spec.Size
	}
	return node, nil
}

// ResumeDownload serves the requested file's bytes from downloadContent in
// resumeDLChunkSize-sized chunks, modelling an offset-addressed server, so the keep-both
// resolver's large-node resume branch can be exercised without a network.
func (c *pushClient) ResumeDownload(_ context.Context, _, fileID string, _, _, offset int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.resumeDLCalls++
	c.resumeDLOffsets = append(c.resumeDLOffsets, offset)
	c.mu.Unlock()

	body := c.downloadContent[fileID]
	end := int64(len(body))
	if c.resumeDLChunkSize > 0 && offset+c.resumeDLChunkSize < end {
		end = offset + c.resumeDLChunkSize
	}
	chunk := body[offset:end]
	if _, err := w.Write(chunk); err != nil {
		return actions.Node{}, err
	}
	return actions.Node{FileID: fileID, Hash: md4.ContentHash(chunk), Size: int64(len(chunk)), MTime: c.downloadNode[fileID].MTime}, nil
}

// outboundEngine wires a store, a concTransport over the given pushClient, and a temp
// sync root with the view directory seeded, returning the engine, the store, the view
// directory and the client.
func outboundEngine(t *testing.T, c *pushClient, viewName, viewNodeID string) (*Engine, *store.Store, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: viewName, NodeID: viewNodeID}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, viewName)
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	return e, st, viewDir
}

func TestOutboundUploadSmallCreate(t *testing.T) {
	c := &pushClient{uploadNode: map[string]actions.Node{"a.txt": {FileID: "newf", Hash: "h1", MTime: 123}}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "h1", LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: "h1", LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "root1" || u.name != "a.txt" || string(u.content) != "hello" {
		t.Errorf("upload call = %+v, want parent root1 name a.txt content hello", u)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	// The baseline records the hash of the bytes actually uploaded (md4 of "hello"), not the
	// scan-time op.LocalHash, so the next scan does not read a spurious modify.
	wantLocalHash := md4.ContentHash([]byte("hello"))
	if b.State != store.StateSynced || b.FileID != "newf" || b.ServerHash != "h1" || b.LocalHash != wantLocalHash || b.LocalSize != 5 {
		t.Errorf("baseline = %+v, want synced newf/h1 localHash %s", b, wantLocalHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundModifyUploadErrnoLessRejectionLeavesQueued exercises the engine side of
// errno-less reply rejection: the actions layer now rejects an errno-less Upload reply as
// ErrUnexpectedReply (a flaky/non-conforming DSM answering {} after draining both frames). The
// modify branch must NOT record the phantom push - the op stays queued and the baseline stays
// StatePending (never StateSynced with the op's backfilled id), so the edit re-uploads once
// the server behaves, instead of diverging silently forever.
func TestOutboundModifyUploadErrnoLessRejectionLeavesQueued(t *testing.T) {
	c := &pushClient{uploadErr: actions.ErrUnexpectedReply}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("edited"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// A modify: the baseline already has the server file id and is StatePending awaiting push.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", FileID: "existf", LocalHash: "oldh", LocalSize: 6, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "existf", LocalHash: "newh", LocalSize: 6, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want a transient error for the rejected errno-less upload, got nil")
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateSynced {
		t.Errorf("baseline marked synced on an errno-less upload reply: %+v", b)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("op dead-lettered on a transient errno-less rejection: %d", dl)
	}
}

// TestOutboundModifyUploadSparseErrnoZeroBackfillsID is the errno-less rejection's regression
// guard: a legitimate
// sparse success (errno == 0 but no node fields echoed, so Upload returns an empty Node)
// must still complete the modify - the engine backfills the file id from the op and marks
// the baseline StateSynced, exactly as before the fix. The fix rejects only an ABSENT errno,
// never a present errno with missing node fields.
func TestOutboundModifyUploadSparseErrnoZeroBackfillsID(t *testing.T) {
	// uploadNode has no entry for "a.txt", so pushClient.Upload returns a zero Node with a
	// nil error - the fake stand-in for a real errno=0 reply that decodes to Node{}.
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", FileID: "existf", LocalHash: "oldh", LocalSize: 5, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "existf", LocalHash: "newh", LocalSize: 5, LocalMTime: 100, UnixPerm: 644,
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
	wantLocalHash := md4.ContentHash([]byte("hello"))
	// The id is backfilled from the op (the reply echoed none), the baseline advances to
	// synced, and it records the hash of the bytes actually uploaded.
	if b.State != store.StateSynced || b.FileID != "existf" || b.LocalHash != wantLocalHash {
		t.Errorf("baseline = %+v, want synced existf localHash %s", b, wantLocalHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

func TestOutboundUploadLargeResume(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "bh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// A file larger than the resume threshold so the chunked path is taken.
	size := uploadResumeThreshold + 1234
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	bigPath := filepath.Join(viewDir, "big.bin")
	if err := os.WriteFile(bigPath, content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The op carries the scan-time facts of the unchanged file: its real hash and on-disk mtime.
	// With both matching, the apply-time re-derive guard stays quiet and the recorded hash is sent.
	fi, err := os.Stat(bigPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	liveHash := md4.ContentHash(content)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: liveHash, LocalSize: int64(size), LocalMTime: fi.ModTime().Unix(), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 0 {
		t.Errorf("small Upload used for large file: %d calls", len(c.uploads))
	}
	if c.resumeSpec.ParentID != "root1" || c.resumeSpec.Name != "big.bin" || c.resumeSpec.Size != int64(size) || c.resumeSpec.Hash != liveHash {
		t.Errorf("resume token request = %+v", c.resumeReq)
	}
	if !bytes.Equal(c.resumeChunks, content) {
		t.Errorf("streamed %d bytes, want %d (content mismatch)", len(c.resumeChunks), size)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "bigf" {
		t.Errorf("baseline = %+v, want synced bigf", b)
	}
}

// TestOutboundUploadResumeAlreadyCommitted models a resume token whose reported offset
// already equals the whole file size (a prior attempt streamed every byte but its
// completing reply was lost). The push must still obtain the node facts by re-sending the
// final chunk rather than wedging the op forever.
func TestOutboundUploadResumeAlreadyCommitted(t *testing.T) {
	size := int64(uploadResumeThreshold + 1234)
	c := &pushClient{
		// Offset == size: the server claims it already holds the entire file.
		resumeToken: actions.ResumeToken{Token: "tk", Offset: size},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "bh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "bh", LocalSize: size, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The recovery must re-send the FINAL chunk with the correct bytes at the correct
	// offset: rewind to the last chunk boundary (size - uploadChunkSize) and re-stream from
	// there. The server-assembled buffer's final-chunk region must reconstruct the file
	// tail exactly - a corrupted rewind (wrong offset or length) would land the wrong bytes
	// here, which the production server's hash check would reject.
	rewind := size - int64(uploadChunkSize)
	if !bytes.Equal(c.resumeAssembled[rewind:], content[rewind:]) {
		t.Errorf("re-sent final chunk did not reconstruct the file tail: the rewind desynced from the offset")
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "bigf" {
		t.Errorf("baseline = %+v, want synced bigf", b)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v): want empty queue after already-committed resume", n, perr)
	}
}

// TestOutboundUploadResumeFileShrankInWindow models a large file that shrank between the
// scan that enqueued the op (op.LocalSize) and the apply (the smaller on-disk size). The
// resume push must transfer the live bytes - bounding its read by op.LocalSize would read
// past EOF and, since the durable queue keeps the stale size, wedge the path forever.
func TestOutboundUploadResumeFileShrankInWindow(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "sh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// The file on disk is large enough for the resume path but smaller than the scan-time
	// op.LocalSize: it was truncated after the scan recorded the larger size.
	live := int64(uploadResumeThreshold + 100)
	content := make([]byte, live)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	staleSize := live + 5000
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "stale", LocalSize: staleSize, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	wantHash := md4.ContentHash(content)
	if c.resumeSpec.Size != live || c.resumeSpec.Hash != wantHash {
		t.Errorf("resume token request = %+v, want live size %d hash %s", c.resumeReq, live, wantHash)
	}
	if !bytes.Equal(c.resumeChunks, content) {
		t.Errorf("streamed %d bytes, want the %d live bytes", len(c.resumeChunks), live)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "bigf" || b.LocalSize != live || b.LocalHash != wantHash {
		t.Errorf("baseline = %+v, want synced bigf with live size %d hash %s", b, live, wantHash)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v): want empty queue, not a wedged op", n, perr)
	}
}

// TestOutboundUploadResumeSameSizeContentChangedInWindow models a large file rewritten to
// the identical length between the scan that enqueued the op (op.LocalHash) and the apply.
// The size guard alone would not fire, so the resume push would advertise the stale scan-time
// hash while streaming the new bytes - the server's completion check rejects the mismatch and,
// since the durable queue keeps the stale hash, the path wedges forever. The mtime change must
// trigger a live re-hash so the advertised hash matches the streamed bytes.
func TestOutboundUploadResumeSameSizeContentChangedInWindow(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "sh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	size := int64(uploadResumeThreshold + 100)
	content := make([]byte, size)
	for i := range content {
		content[i] = byte((i + 7) % 251)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Same size as the on-disk file, but a stale hash and an old mtime (1970): the content was
	// overwritten in-window without changing the byte count.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "stale", LocalSize: size, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	wantHash := md4.ContentHash(content)
	if c.resumeSpec.Hash != wantHash {
		t.Errorf("resume token advertised hash = %q, want live hash %s", c.resumeSpec.Hash, wantHash)
	}
	if !bytes.Equal(c.resumeChunks, content) {
		t.Errorf("streamed %d bytes, want the %d live bytes", len(c.resumeChunks), size)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != wantHash {
		t.Errorf("baseline = %+v, want synced with live hash %s", b, wantHash)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v): want empty queue, not a wedged op", n, perr)
	}
}

// TestOutboundUploadResumeGrewInWindow models a large file that GREW between the fstat that
// captured liveSize and the content hash (a concurrent append in the scan→apply window). The
// resume path streams exactly liveSize bytes (for offset < size), so an advertised hash that
// covered every byte to EOF would describe more bytes than are sent and the server's
// completion check would never converge - the op would fail on every drain until the writer
// stops. The fix hashes io.LimitReader(f, liveSize), so the advertised hash always equals the
// streamed bytes regardless of any growth.
func TestOutboundUploadResumeGrewInWindow(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "sh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// The file on disk at fstat time is exactly the resume-threshold-plus size.
	live := int64(uploadResumeThreshold + 100)
	content := make([]byte, live)
	for i := range content {
		content[i] = byte(i % 251)
	}
	bigPath := filepath.Join(viewDir, "big.bin")
	if err := os.WriteFile(bigPath, content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Inject a concurrent append AFTER fstat captures liveSize but BEFORE the hash: the file
	// grows by a tail the resume stream will never send. The old code (hash to EOF) would
	// advertise a hash over live+tail bytes while streaming only live bytes.
	tail := bytes.Repeat([]byte{0xAB}, 4096)
	e.afterUploadFstat = func() {
		f, err := os.OpenFile(bigPath, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Errorf("append-open: %v", err)
			return
		}
		if _, err := f.Write(tail); err != nil {
			t.Errorf("append-write: %v", err)
		}
		if err := f.Close(); err != nil {
			t.Errorf("append-close: %v", err)
		}
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "stale", LocalSize: live, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The advertised hash must cover exactly the liveSize bytes that were streamed - not the
	// grown file - so the streamed bytes and the advertised hash are in step.
	wantHash := md4.ContentHash(content)
	if c.resumeSpec.Size != live {
		t.Errorf("resume token size = %d, want liveSize %d", c.resumeSpec.Size, live)
	}
	if c.resumeSpec.Hash != wantHash {
		t.Errorf("advertised hash = %q, want hash of exactly liveSize bytes %q (covered the appended tail?)", c.resumeSpec.Hash, wantHash)
	}
	if !bytes.Equal(c.resumeChunks, content) {
		t.Errorf("streamed %d bytes, want exactly the %d liveSize bytes", len(c.resumeChunks), live)
	}
	if c.resumeSpec.Hash != md4.ContentHash(c.resumeChunks) {
		t.Errorf("advertised hash does not match streamed bytes: server completion check would never converge")
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != wantHash || b.LocalSize != live {
		t.Errorf("baseline = %+v, want synced with liveSize hash %s", b, wantHash)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v): want empty queue, not a wedged op", n, perr)
	}
}

func TestOutboundMkdirRecordsID(t *testing.T) {
	c := &pushClient{mkdirNode: map[string]actions.Node{"sub": {FileID: "d1", MTime: 7}}}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub",
		LocalMTime: 100, UnixPerm: 755,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.mkdirs) != 1 || c.mkdirs[0].parentID != "root1" || c.mkdirs[0].name != "sub" {
		t.Fatalf("mkdir calls = %+v", c.mkdirs)
	}
	b, err := st.GetBaseline(7, "sub")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileType != "dir" || b.FileID != "d1" {
		t.Errorf("baseline = %+v, want synced dir d1", b)
	}
}

func TestOutboundRemove(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.removes) != 1 || c.removes[0] != "gone.txt" {
		t.Fatalf("remove calls = %+v", c.removes)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived the remove")
	}
}

func TestOutboundRemoveIdempotentOnPathError(t *testing.T) {
	// The node is genuinely gone, so the corroborating probe of its own path answers with
	// the path-error sentinel too.
	c := &pushClient{
		removeErr:          actions.ErrPathError,
		queryNodeErrByPath: map[string]error{"gone.txt": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived an already-gone remove")
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained on path error: %d", n)
	}
}

func TestOutboundRename(t *testing.T) {
	c := &pushClient{queryNodeErrByPath: renameDestFree("new.txt")}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// localReconcile relocated the baseline to NewPath (pending) when it emitted the op.
	if err := st.PutBaseline(7, "new.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9", LocalHash: "h9", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.renames) != 1 || c.renames[0] != [2]string{"old.txt", "new.txt"} {
		t.Fatalf("rename calls = %+v", c.renames)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f9" || b.LocalHash != "h9" {
		t.Errorf("baseline = %+v, want synced f9/h9", b)
	}
}

// TestOutboundRenameCrashBeforeBaselineMove covers the window where a crash lands between the
// durable Enqueue and reconcile's MoveBaseline (separate transactions): the source baseline is
// still synced at op.Path and there is no record at NewPath. The rename must still complete
// with the moved node's file id carried to NewPath and the stranded source baseline dropped,
// rather than writing a fresh NewPath baseline with an empty file id (which would drop the node
// from byid) and leaving a synced orphan at op.Path.
func TestOutboundRenameCrashBeforeBaselineMove(t *testing.T) {
	c := &pushClient{queryNodeErrByPath: renameDestFree("new.txt")}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// Simulate the crash: the baseline never moved off op.Path; nothing exists at NewPath.
	if err := st.PutBaseline(7, "old.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StateSynced}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9", LocalHash: "h9", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline new.txt: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f9" || b.FileType != "file" || b.LocalHash != "h9" {
		t.Errorf("new.txt baseline = %+v, want synced f9/file/h9", b)
	}
	// The stranded source baseline must be gone (PutBaseline's single-Put move logic drops it).
	if _, err := st.GetBaseline(7, "old.txt"); !isNotFound(err) {
		t.Errorf("old.txt baseline still present (err=%v); want dropped", err)
	}
	// byid for f9 must now resolve to the new path, not the stale source.
	if p, err := st.PathByID(7, "f9"); err != nil || p != "new.txt" {
		t.Errorf("PathByID(f9) = %q, %v; want new.txt", p, err)
	}
}

// TestOutboundUploadConflictEnqueuesResolve confirms an upload rejected with stale-base
// 24579 enqueues exactly one OpResolveConflict for its view+path (carrying UnixPerm) and
// acks the original upload, rather than marking StateConflict. A single batch is drained so
// the just-enqueued resolve op can be inspected before it runs.
func TestOutboundUploadConflictEnqueuesResolve(t *testing.T) {
	c := &pushClient{uploadErr: actions.ErrConflictVersion}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "c.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "c.txt", store.Baseline{
		FileID: "cf", FileType: "file", ServerHash: "old", LocalHash: "new", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "c.txt",
		FileID: "cf", LocalHash: "new", LocalSize: 1, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Drain only the upload batch so the enqueued resolve op stays queued for inspection.
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if err := e.drainBatch(context.Background(), ops, &blockedKeys{}, &deadOutputs{}); err != nil {
		t.Fatalf("drainBatch: %v", err)
	}

	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after drain: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("queue has %d ops, want exactly one resolve (upload acked)", len(pending))
	}
	r := pending[0]
	if r.Type != store.OpResolveConflict || r.ViewID != 7 || r.Path != "c.txt" || r.UnixPerm != 644 {
		t.Errorf("queued op = %+v, want resolve_conflict for 7/c.txt with perm 644", r.Op)
	}
	// The resolve op must carry the upload's server file id so the resolver's
	// server-vanished disambiguation keys on the stable id, not the leaf name.
	if r.FileID != "cf" {
		t.Errorf("resolve op FileID = %q, want %q carried from the upload", r.FileID, "cf")
	}
	if r.Direction != store.DirOutbound {
		t.Errorf("resolve op direction = %q, want outbound", r.Direction)
	}
	// No StateConflict marking on this path - the funnel replaces that for uploads.
	b, err := st.GetBaseline(7, "c.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State == store.StateConflict {
		t.Errorf("baseline marked conflict; want left pending for the resolver")
	}
}

// TestOutboundUploadConflictFunnelsToKeepBoth drives the full funnel end-to-end: the
// canonical push hits 24579, an OpResolveConflict is enqueued and acked, and the resolver
// downloads the server version onto the canonical path, preserves the divergent local
// content as a _Conflict copy uploaded back as a new file, advances both baselines to
// synced, and records a durable Conflict.
func TestOutboundUploadConflictFunnelsToKeepBoth(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-side-newer")
	localBytes := []byte("local-divergent-content")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("c.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// The first Upload (the canonical push) hits a diverged remote; the later copy
		// upload succeeds.
		uploadConflictLeft: 1,
		queryNode: map[string]actions.Node{
			"c.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "c.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The baseline agrees with the node the server reports (same id, same content hash), so the
	// pre-turn divergence probe (finding H1) finds nothing to refuse and the push goes out -
	// keeping this test on the arm it was written for: the SERVER's own 24579 verdict, which the
	// measured "compare_mtime" policy still produces for an older request mtime. The local file
	// diverges from both, which is what the resolver then keeps both of.
	if err := st.PutBaseline(7, "c.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: serverHash, LocalHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "c.txt",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(localBytes)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path now holds the server bytes; the copy holds the old local bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "c.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// Exactly one (successful) upload was recorded: the copy. The canonical push failed
	// before recording.
	if len(c.uploads) != 1 || c.uploads[0].name != copyName || string(c.uploads[0].content) != string(localBytes) {
		t.Fatalf("uploads = %+v, want one copy upload of %q", c.uploads, copyName)
	}
	cb, err := st.GetBaseline(7, "c.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" || cb.LocalHash != serverHash {
		t.Errorf("canonical baseline = %+v, want synced srv1/%s", cb, serverHash)
	}
	pb, err := st.GetBaseline(7, copyRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if pb.State != store.StateSynced || pb.FileID != "copyf" || pb.LocalHash != localHash {
		t.Errorf("copy baseline = %+v, want synced copyf/%s", pb, localHash)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "c.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one c.txt->%s", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundRenameConflictWithoutBaselineSynthesizesRecord confirms that when a conflicted
// non-upload push (a rename) has no baseline at its destination - the crash/error window
// between localReconcile's enqueue and its baseline relocation - markOutboundConflict
// synthesises a StateConflict baseline from the op's own facts instead of silently acking.
// Without it the path stays baseline-less and the next scan re-detects it as new, looping
// forever. Uploads now funnel to the keep-both resolver, so the rename path is
// where this synthesis remains reachable.
func TestOutboundRenameConflictWithoutBaselineSynthesizesRecord(t *testing.T) {
	c := &pushClient{renameErr: actions.ErrConflictVersion, queryNodeErrByPath: renameDestFree("new.txt")}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// Deliberately enqueue the rename with NO baseline at NewPath (the crash window).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9", LocalHash: "h1", LocalSize: 1, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("conflict baseline was not synthesised: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want conflict", b.State)
	}
	if b.FileType != "file" || b.FileID != "f9" || b.LocalHash != "h1" || b.LocalSize != 1 || b.UnixPerm != 644 {
		t.Errorf("synthesised baseline = %+v, want op facts carried", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("conflicted op not acked: %d remain", n)
	}
}

// TestOutboundChildOfConflictedParentDeadLetters confirms a child op whose parent directory is
// stuck in StateConflict (a rejected parent mkdir that never got a server id) is itself marked
// StateConflict and dead-lettered, rather than retrying resolveParentID forever and wedging the
// queue.
func TestOutboundChildOfConflictedParentDeadLetters(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "sub", "child.txt"), []byte("y"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Parent dir conflicted: present baseline, StateConflict, no server id.
	if err := st.PutBaseline(7, "sub", store.Baseline{
		FileType: "dir", State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "sub/child.txt",
		LocalHash: "h1", LocalSize: 1,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain completes (no wedge) and the op is gone from the queue.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("child of conflicted parent not dead-lettered: %d remain", n)
	}
	if len(c.uploads) != 0 {
		t.Errorf("child was uploaded despite conflicted parent: %+v", c.uploads)
	}
	b, err := st.GetBaseline(7, "sub/child.txt")
	if err != nil {
		t.Fatalf("child baseline missing: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("child baseline state = %q, want conflict", b.State)
	}
}

// TestOutboundUploadSmallReplaySafeOnPathError confirms a small upload whose prior attempt
// committed server-side before its reply was lost is replay-safe: a retry that hits
// ErrPathError verifies the postcondition (the node now at the path carries OUR content
// hash) and adopts it as success instead of wedging on the path error.
func TestOutboundUploadSmallReplaySafeOnPathError(t *testing.T) {
	content := []byte("hello")
	wantHash := md4.ContentHash(content)
	c := &pushClient{
		uploadErr: actions.ErrPathError,
		queryNode: map[string]actions.Node{"a.txt": {FileID: "existf", Hash: wantHash, MTime: 55}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: wantHash, LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: wantHash, LocalSize: int64(len(content)), UnixPerm: 644,
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
	if b.State != store.StateSynced || b.FileID != "existf" {
		t.Errorf("baseline = %+v, want synced adopting the existing node existf", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("op not acked after replay recovery: %d remain", n)
	}
}

// TestOutboundUploadSmallRealConflictNotMaskedByQueryNode confirms the replay-safety probe
// does NOT swallow a genuine divergence: when the node at the path carries DIFFERENT content
// than we sent, the postcondition's hash gate fails and the original ErrConflictVersion
// surfaces - so the push funnels into the keep-both resolver (an OpResolveConflict is
// enqueued) rather than being falsely adopted as a successful, synced upload of the foreign
// node. One batch is drained so the funnel decision is observed directly.
func TestOutboundUploadSmallRealConflictNotMaskedByQueryNode(t *testing.T) {
	content := []byte("ours")
	c := &pushClient{
		uploadErr: actions.ErrConflictVersion,
		// The server node at the path holds someone else's content (a different hash).
		queryNode: map[string]actions.Node{"c.txt": {FileID: "cf", Hash: "theirhash"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "c.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "c.txt", store.Baseline{
		FileID: "cf", FileType: "file", ServerHash: "old", LocalHash: md4.ContentHash(content), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "c.txt",
		FileID: "cf", LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Drain only the upload batch so the funnel's outcome (a queued resolve op, not a
	// falsely-synced baseline) is visible before the resolver itself runs.
	ops, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if err := e.drainBatch(context.Background(), ops, &blockedKeys{}, &deadOutputs{}); err != nil {
		t.Fatalf("drainBatch: %v", err)
	}
	b, err := st.GetBaseline(7, "c.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	// The foreign node must NOT have been adopted as a successful synced upload.
	if b.State == store.StateSynced {
		t.Errorf("baseline synced from the probe's foreign node; a real divergence must not be masked")
	}
	pending, err := st.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after drain: %v", err)
	}
	if len(pending) != 1 || pending[0].Type != store.OpResolveConflict || pending[0].Path != "c.txt" {
		t.Errorf("queued ops = %+v, want one resolve_conflict for c.txt", pending)
	}
}

func TestOutboundParentBeforeChild(t *testing.T) {
	c := &pushClient{
		mkdirNode:  map[string]actions.Node{"dir": {FileID: "d1"}},
		uploadNode: map[string]actions.Node{"f.txt": {FileID: "f1", Hash: "h"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "f.txt"), []byte("body"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Parent mkdir enqueued before the child upload; the child's parent id is unknown
	// until the mkdir records it. The conflict-key serialisation runs the mkdir first.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "dir", LocalMTime: 1,
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "dir/f.txt",
		LocalHash: "h", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 || c.uploads[0].parentID != "d1" {
		t.Fatalf("upload parent = %+v, want resolved to mkdir's d1", c.uploads)
	}
	b, err := st.GetBaseline(7, "dir/f.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f1" {
		t.Errorf("child baseline = %+v, want synced f1", b)
	}
}

func TestOutboundParentUnknownLeavesQueued(t *testing.T) {
	// A nested upload whose parent baseline has no server id yet leaves the op queued.
	c := &pushClient{}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos", "dir"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "Photos", "dir", "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Parent "dir" baseline exists but carries no FileID (server id not yet assigned).
	if err := st.PutBaseline(7, "dir", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "dir/f.txt", LocalSize: 1,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The unresolved-parent op surfaces an error and stays queued for the next drain.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error for unresolved parent, got nil")
	}
	if len(c.uploads) != 0 {
		t.Errorf("upload attempted despite unknown parent: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d", n)
	}
}

func TestOutboundBrokenConnRetry(t *testing.T) {
	c := &pushClient{
		uploadFailsLeft: 1,
		uploadNode:      map[string]actions.Node{"a.txt": {FileID: "f1", Hash: "h"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: "h", LocalSize: 2,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First drain hits the broken connection and leaves the op queued.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("first drain: want broken-conn error, got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("op not left queued after broken conn: %d", n)
	}
	// The next drain (the re-dialed connection) succeeds.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f1" {
		t.Errorf("baseline = %+v, want synced f1", b)
	}
}

func TestOutboundRejectedPathDeadLettered(t *testing.T) {
	// A structurally invalid outbound path can never be pushed: the op is acked (dropped)
	// so it cannot wedge the queue, no server call is made, and the pending baseline is
	// left in place (inert - the next scan only acts on StateSynced records).
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "../escape", store.Baseline{FileID: "bad", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "../escape", FileID: "bad",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Dead-lettering is not a drain error: the op is dropped and the queue keeps flowing.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.removes) != 0 {
		t.Errorf("remove attempted for rejected path: %+v", c.removes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("rejected op not acked: %d remain", n)
	}
	if _, err := st.GetBaseline(7, "../escape"); err != nil {
		t.Errorf("pending baseline should be left in place, got err: %v", err)
	}
	// The drop must leave a durable dead-letter trace so status reports it.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped outbound op", n, err)
	}
}

func TestOutboundMkdirRejectedPathDeadLettered(t *testing.T) {
	// A traversal mkdir path must dead-letter, not wedge as errParentUnknown: the containment
	// check has to run before resolveParentID (whose path.Dir of "../escape" is ".." and
	// would otherwise return errParentUnknown, leaving the op queued forever).
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "../escape",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.mkdirs) != 0 {
		t.Errorf("mkdir attempted for rejected path: %+v", c.mkdirs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("rejected mkdir op not acked: %d remain", n)
	}
}

func TestOutboundRenameRejectedPathDeadLettered(t *testing.T) {
	// A rename whose source OR destination escapes the sync root must dead-letter rather than
	// being sent to the server. Both endpoints are validated, so each escape is exercised.
	for _, tc := range []struct {
		name     string
		from, to string
	}{
		{name: "source escapes", from: "../escape", to: "new.txt"},
		{name: "destination escapes", from: "old.txt", to: "../escape"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{}
			e, st, _ := outboundEngine(t, c, "Photos", "root1")
			if _, err := st.Enqueue(store.Op{
				Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
				Path: tc.from, NewPath: tc.to, FileID: "f9",
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			if err := e.drainQueue(context.Background()); err != nil {
				t.Fatalf("drainQueue: %v", err)
			}
			if len(c.renames) != 0 {
				t.Errorf("rename attempted for rejected path: %+v", c.renames)
			}
			if n, _ := st.PendingLen(); n != 0 {
				t.Errorf("rejected rename op not acked: %d remain", n)
			}
		})
	}
}

func TestOutboundRenameStaleBaseConflict(t *testing.T) {
	// A stale-base rejection on a rename marks the baseline at the destination (where
	// local-reconcile relocated it) StateConflict and acks the op, never overwriting the
	// diverged remote (keep-both deferred to 6d).
	c := &pushClient{renameErr: actions.ErrConflictVersion, queryNodeErrByPath: renameDestFree("new.txt")}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9", LocalHash: "h9", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %q, want conflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("conflicted rename not acked: %d remain", n)
	}
}

func TestOutboundRemoveTransientFailureRequeued(t *testing.T) {
	// A non-path-error remove failure is transient: the op stays queued for the next drain
	// and the baseline is NOT dropped (the node is still tracked until the delete lands).
	c := &pushClient{removeErr: actions.ErrConnBroken}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want transient remove error, got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped on a transient remove failure: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("transient-failed op not left queued: %d", n)
	}
}

// TestOutboundUploadSourceVanishedFreshCreate models an upload whose local file disappears
// before the push runs. For a fresh create (no server file id) nothing reached the server,
// so the op is dropped and its pending baseline removed - the next scan starts clean rather
// than the deletion staying invisibly wedged behind a queued upload.
func TestOutboundUploadSourceVanishedFreshCreate(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// Baseline pending but the file was never written to disk (it vanished after the scan).
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileType: "file", LocalHash: "h", LocalSize: 3, State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt",
		LocalHash: "h", LocalSize: 3,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("upload attempted for a vanished source: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("vanished upload not dropped: %d remain", n)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("fresh-create baseline should be dropped when its source vanished")
	}
}

// TestOutboundUploadSourceVanishedModify models a modify upload whose file disappears before
// the push. The server still holds the node (op.FileID set), so the op is dropped and the
// baseline reverts to synced - the next scan then sees a synced baseline with no file and
// emits the OpRemove that propagates the deletion, instead of it being lost.
func TestOutboundUploadSourceVanishedModify(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", LocalHash: "new", LocalSize: 9, State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt",
		FileID: "ef", LocalHash: "new", LocalSize: 9,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("upload attempted for a vanished source: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("vanished upload not dropped: %d remain", n)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "ef" {
		t.Errorf("modify baseline = %+v, want reverted to synced ef so the next scan removes it", b)
	}
}

// TestVanishedUploadRecoversDeferredRemoteCreate models reconcile having deferred a remote
// create for a path with a queued local upload (advancing the cursor on the promise the upload
// would funnel through 24579), after which the local source vanished. Dropping the upload would
// lose the acknowledged remote create; instead the server is probed, found to hold a version,
// and the keep-both resolver's source-gone path pulls it onto the canonical path.
func TestVanishedUploadRecoversDeferredRemoteCreate(t *testing.T) {
	serverBytes := []byte("remote-create")
	serverHash := md4.ContentHash(serverBytes)
	c := &pushClient{
		queryNode:       map[string]actions.Node{"new.txt": {FileID: "srvC", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		downloadContent: map[string][]byte{"srvC": serverBytes},
		downloadNode:    map[string]actions.Node{"srvC": {FileID: "srvC", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// Fresh-create pending baseline (no server file id), but the local file vanished.
	if err := st.PutBaseline(7, "new.txt", store.Baseline{FileType: "file", LocalHash: "localh", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "new.txt", LocalHash: "localh", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The deferred remote create was recovered onto the canonical path.
	if got, err := os.ReadFile(filepath.Join(viewDir, "new.txt")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want recovered %q", got, err, serverBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploaded a vanished source: %+v", c.uploads)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "srvC" || b.LocalHash != serverHash {
		t.Errorf("baseline = %+v, want synced srvC/%s adopting the recovered server facts", b, serverHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadRecoversDeferredRemoteModify is the modify counterpart: a remote modify was
// deferred, the local source vanished, and the server now holds a hash diverging from the base.
// The deferred update must be pulled rather than the path being pushed back as a deletion.
func TestVanishedUploadRecoversDeferredRemoteModify(t *testing.T) {
	serverBytes := []byte("remote-modified")
	serverHash := md4.ContentHash(serverBytes)
	c := &pushClient{
		queryNode:       map[string]actions.Node{"edit.txt": {FileID: "ef", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		downloadContent: map[string][]byte{"ef": serverBytes},
		downloadNode:    map[string]actions.Node{"ef": {FileID: "ef", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// Pending modify baseline whose recorded server base diverges from what the server now holds.
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "edit.txt")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want recovered %q", got, err, serverBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploaded a vanished source: %+v", c.uploads)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.ServerHash != serverHash {
		t.Errorf("baseline = %+v, want synced adopting recovered server hash %s", b, serverHash)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadLocalDeleteNotResurrected guards the other side of the vanished-upload revert:
// when the server's version still matches the base (no remote change was deferred), a vanished
// upload is a genuine local delete and must NOT be re-downloaded - that would resurrect a file the
// user deleted. The baseline reverts to synced so the next scan emits the OpRemove instead.
func TestVanishedUploadLocalDeleteNotResurrected(t *testing.T) {
	c := &pushClient{
		// The server still holds the base version (same hash): nothing diverged.
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ef", Hash: "basehash", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The deleted file was not resurrected on disk.
	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("local delete was resurrected (stat err = %v), want the file to stay absent", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "ef" {
		t.Errorf("baseline = %+v, want reverted to synced ef so the next scan emits the remove", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadRecreateSameContentReuploads reproduces a stale-facts bug:
// resolveVanishedUpload used to revert the modify baseline to StateSynced while KEEPING the
// vanished edit's LocalHash/LocalSize/LocalMTime - facts describing content the server never saw.
// A user recreating the file with that same content before the next scan (git checkout, a rebuilt
// artifact) then re-hashed to exactly the stale LocalHash, so no OpUpload fired: local held the
// edit, the server held the old content, and the baseline read "synced" - silent divergence. The
// revert must clear the local facts so an identical-content recreate always re-diffs into an
// OpUpload against the existing server node.
func TestVanishedUploadRecreateSameContentReuploads(t *testing.T) {
	edited := []byte("edited content the server never saw")
	editedHash := md4.ContentHash(edited)
	c := &pushClient{
		// The server still holds the base version (same hash): nothing diverged, so the vanish
		// resolves as a genuine local delete and the baseline reverts to synced.
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ef", Hash: "basehash", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash",
		LocalHash: editedHash, LocalSize: int64(len(edited)), LocalMTime: 777,
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef",
		LocalHash: editedHash, LocalSize: int64(len(edited)),
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// A second in-sync file keeps the post-drain scan non-empty, so the whole-view empty-scan
	// debounce never engages and the recreate/remove diff below is exercised directly.
	keep := []byte("undisturbed sibling")
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keep, 0o644); err != nil {
		t.Fatalf("seed keep.txt: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileID: "kf", FileType: "file", ServerHash: md4.ContentHash(keep),
		LocalHash: md4.ContentHash(keep), LocalSize: int64(len(keep)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keep baseline: %v", err)
	}

	// The upload source is absent on disk: the drain routes through handleVanishedUpload and
	// reverts the baseline to synced.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Fatalf("baseline state = %v, want StateSynced after the revert", b.State)
	}
	if b.LocalHash != "" || b.LocalSize != 0 || b.LocalMTime != 0 {
		t.Errorf("reverted baseline keeps vanished-edit local facts (hash=%q size=%d mtime=%d), want cleared",
			b.LocalHash, b.LocalSize, b.LocalMTime)
	}
	if b.ServerHash != "basehash" || b.FileID != "ef" {
		t.Errorf("revert disturbed server facts: %+v", b)
	}

	// The user recreates the file with the SAME content as the vanished edit before the next
	// scan. The re-hash must diff against the (now empty) baseline hash and re-upload.
	if err := os.WriteFile(filepath.Join(viewDir, "edit.txt"), edited, 0o644); err != nil {
		t.Fatalf("recreate edit.txt: %v", err)
	}
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "edit.txt" {
		t.Fatalf("got %+v, want a single OpUpload for the recreated edit.txt (silent divergence before the fix)", ops)
	}
	if ops[0].FileID != "ef" || ops[0].LocalHash != editedHash {
		t.Errorf("upload op = %+v, want a modify of server node ef carrying hash %s", ops[0], editedHash)
	}
}

// TestVanishedUploadStayDeletedEmitsRemove pins the other leg of the vanished-upload revert: when
// the vanished file is NOT recreated, the reverted synced baseline (with its local facts cleared)
// still diffs as a vanished node on the next scan and emits the OpRemove that propagates the
// genuine local delete - clearing the facts must not disturb the remove path.
func TestVanishedUploadStayDeletedEmitsRemove(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ef", Hash: "basehash", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "editedhash", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "editedhash",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// Keep the view non-empty so the empty-scan debounce stays out of the way (it would
	// otherwise suppress the remove for EmptyScanThreshold consecutive scans).
	keep := []byte("undisturbed sibling")
	if err := os.WriteFile(filepath.Join(viewDir, "keep.txt"), keep, 0o644); err != nil {
		t.Fatalf("seed keep.txt: %v", err)
	}
	if err := st.PutBaseline(7, "keep.txt", store.Baseline{
		FileID: "kf", FileType: "file", ServerHash: md4.ContentHash(keep),
		LocalHash: md4.ContentHash(keep), LocalSize: int64(len(keep)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed keep baseline: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpRemove || ops[0].Path != "edit.txt" {
		t.Fatalf("got %+v, want a single OpRemove propagating the genuine local delete", ops)
	}
}

// TestVanishedUploadServerGoneNotResurrected covers the server-gone branch of
// serverDivergedForVanishedUpload: the upload source vanished and QueryNode reports ErrPathError,
// which is ambiguous (absent or a timeout). A clean parent listing that omits the node proves it
// gone server-side - a genuine local delete, NOT a deferred remote change - so the file must not be
// re-downloaded and the modify baseline reverts to synced for the next scan to emit the remove.
func TestVanishedUploadServerGoneNotResurrected(t *testing.T) {
	c := &pushClient{
		queryNodeErr: actions.ErrPathError, // ambiguous: absent or a server-side timeout
		// The parent listing succeeds and omits edit.txt: unambiguous proof of absence.
		listResult: map[string][]actions.Node{"": {{Name: "other.txt", FileID: "f9"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("a server-gone vanished upload was resurrected on disk (stat err = %v)", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %v, want StateSynced so the next scan emits the remove", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadServerIsDirNotResurrected covers the server-is-directory branch: the upload
// source vanished and the server now holds a directory at the path. That is out of keep-both's
// file-content scope, so it is treated as a local delete - the file must not be re-downloaded and
// the baseline reverts to synced rather than the op being funnelled into the resolver.
func TestVanishedUploadServerIsDirNotResurrected(t *testing.T) {
	c := &pushClient{
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ed", IsDir: true}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("a server-is-dir vanished upload was resurrected on disk (stat err = %v)", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %v, want StateSynced", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadInconclusiveProbeRequeued covers the inconclusive-probe branch: the upload
// source vanished, QueryNode reports ErrPathError AND the parent listing also fails (a server
// timing out every call). The deletion is unconfirmed, so the op must stay queued for retry rather
// than being dropped - dropping it would lose a real remote change still present on the server.
func TestVanishedUploadInconclusiveProbeRequeued(t *testing.T) {
	c := &pushClient{
		queryNodeErr: actions.ErrPathError, // ambiguous: absent or a timeout
		listErr:      actions.ErrPathError, // and the parent listing is inconclusive too
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the deletion is unconfirmed, got nil")
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("an inconclusive-probe vanished upload was resurrected on disk (stat err = %v)", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %v, want StatePending preserved for retry", b.State)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued for retry: %d", n)
	}
}

// TestVanishedUploadMissingHashSameIDNotResurrected covers the missing-hash branch for a modify:
// the upload source vanished and the server still holds the same node (matching file id) but its
// QueryNode reply omits the hash (Node.Hash is tolerantly decoded/optional). A missing hash carries
// no content signal and must NOT be read as divergence - a matching id means the unchanged node, so
// the local delete sticks rather than resurrecting the file.
func TestVanishedUploadMissingHashSameIDNotResurrected(t *testing.T) {
	c := &pushClient{
		// Same file id as the base, but the reply omits the hash.
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ef", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("a missing-hash same-id vanished upload was resurrected on disk (stat err = %v)", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "ef" {
		t.Errorf("baseline = %+v, want reverted to synced ef so the next scan emits the remove", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadEmptyBaseHashSameIDNotResurrected covers the empty-base branch for a modify:
// the upload source vanished and the server still holds the same node (matching file id) WITH a
// hash, but the pending baseline's ServerHash is empty (an earlier upload whose reply omitted the
// hash, preserved through a later modify). A present server hash against an empty base carries no
// reliable content signal and must NOT be read as divergence - comparing server.Hash != "" would
// always be true and resurrect a genuinely deleted file. The decision falls to node identity: the
// matching id is the unchanged node, so the local delete sticks.
func TestVanishedUploadEmptyBaseHashSameIDNotResurrected(t *testing.T) {
	c := &pushClient{
		// Same file id as the base, and the reply DOES carry a hash.
		queryNode: map[string]actions.Node{"edit.txt": {FileID: "ef", Hash: "serverh", MTime: 555, Size: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		// ServerHash empty: an earlier upload reply omitted the hash.
		FileID: "ef", FileType: "file", ServerHash: "", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if _, err := os.Stat(filepath.Join(viewDir, "edit.txt")); !os.IsNotExist(err) {
		t.Errorf("an empty-base same-id vanished upload was resurrected on disk (stat err = %v)", err)
	}
	b, err := st.GetBaseline(7, "edit.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "ef" {
		t.Errorf("baseline = %+v, want reverted to synced ef so the next scan emits the remove", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestVanishedUploadMissingHashDifferentIDRecovered is the counterpart: the server reply omits the
// hash but reports a DIFFERENT file id at the path - a server-side replacement we cannot match to
// our base. With no content signal to confirm a genuine delete, the replacement is preserved through
// the keep-both resolver (downloaded onto the canonical path) rather than pushed back as a deletion.
func TestVanishedUploadMissingHashDifferentIDRecovered(t *testing.T) {
	serverBytes := []byte("server-replacement")
	c := &pushClient{
		// Different file id, hash omitted.
		queryNode:       map[string]actions.Node{"edit.txt": {FileID: "ef2", MTime: 777, Size: int64(len(serverBytes))}},
		downloadContent: map[string][]byte{"ef2": serverBytes},
		downloadNode:    map[string]actions.Node{"ef2": {FileID: "ef2", MTime: 777, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "edit.txt", store.Baseline{
		FileID: "ef", FileType: "file", ServerHash: "basehash", LocalHash: "localnew", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "edit.txt", FileID: "ef", LocalHash: "localnew", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "edit.txt")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want recovered replacement %q", got, err, serverBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploaded a vanished source: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestOutboundUploadSourceSwappedToSymlink models a regular file replaced by a symlink in
// the scan→apply window. openUploadSource refuses to follow it (so its target's bytes can
// never be uploaded outside the sync root) and surfaces errUploadSourceGone, so the op is
// dropped and the baseline reconciled rather than the link being read.
func TestOutboundUploadSourceSwappedToSymlink(t *testing.T) {
	requireSymlinks(t)
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// A secret file outside the sync root the symlink would otherwise exfiltrate.
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("top secret"), 0o600); err != nil {
		t.Fatalf("seed secret: %v", err)
	}
	if err := os.Symlink(secret, filepath.Join(viewDir, "a.txt")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileType: "file", LocalHash: "h", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", LocalHash: "h",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploaded a symlinked source (escape): %+v", c.uploads)
	}
	if len(c.resumeChunks) != 0 {
		t.Errorf("streamed a symlinked source via resume (escape): %d bytes", len(c.resumeChunks))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("symlinked-source op not dropped: %d remain", n)
	}
}

// TestOpenUploadSourceRefusesIntermediateSymlinkEscape verifies that openUploadSource,
// opening through an os.Root, refuses a path whose INTERMEDIATE component is a symlink
// escaping the sync root - the gap O_NOFOLLOW (final component only) leaves open and that
// abs/noSymlinkEscape closes only at check time, not at open time. The source
// must not be opened, so its bytes can never be uploaded outside the root.
func TestOpenUploadSourceRefusesIntermediateSymlinkEscape(t *testing.T) {
	requireSymlinks(t)
	root := t.TempDir()
	// A secret tree outside the sync root the symlinked intermediate dir would expose.
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "f.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatalf("seed secret: %v", err)
	}
	// Inside the root, "sub" is a symlink to the outside tree, so sub/f.txt resolves outside.
	if err := os.Symlink(outside, filepath.Join(root, "sub")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	e := New(newTestStore(t), &concTransport{client: &pushClient{}}, root, "sess", nil)
	f, _, err := e.openUploadSource(filepath.Join(root, "sub", "f.txt"))
	if err == nil {
		_ = f.Close()
		t.Fatal("openUploadSource followed an intermediate escaping symlink; want refusal")
	}
}

// TestOutboundUploadResumeSameSecondRewrite models a large file rewritten to the identical
// length AND the identical mtime second as the scan-time snapshot, with a stale recorded
// hash. A second-resolution mtime check cannot see the change, so the large path must
// re-hash the live content unconditionally; otherwise the stale hash advertised to the
// server fails its completion check forever.
func TestOutboundUploadResumeSameSecondRewrite(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "sh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	size := int64(uploadResumeThreshold + 100)
	content := make([]byte, size)
	for i := range content {
		content[i] = byte((i + 3) % 251)
	}
	bigPath := filepath.Join(viewDir, "big.bin")
	if err := os.WriteFile(bigPath, content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	fi, err := os.Stat(bigPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// The op carries the SAME size and SAME mtime as the live file but a STALE hash - the
	// content was overwritten in-window without changing the byte count or the mtime second.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "stale", LocalSize: size, LocalMTime: fi.ModTime().Unix(), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	wantHash := md4.ContentHash(content)
	if c.resumeSpec.Hash != wantHash {
		t.Errorf("resume advertised hash = %q, want live re-hash %s (same-second rewrite)", c.resumeSpec.Hash, wantHash)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.LocalHash != wantHash {
		t.Errorf("baseline = %+v, want synced with live hash %s", b, wantHash)
	}
}

// TestOutboundUploadGrewLargeUsesResume models a file recorded as small at scan time
// (op.LocalSize <= threshold) that grew beyond the threshold before the push. The
// small-vs-resume decision must use the LIVE size so the now-large file streams through the
// chunked resume path instead of being slurped whole by os.ReadFile.
func TestOutboundUploadGrewLargeUsesResume(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "gf", Hash: "gh", MTime: 9},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	live := int64(uploadResumeThreshold + 500)
	content := make([]byte, live)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "grew.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The op's recorded size is small (the file grew after the scan).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "grew.bin",
		LocalHash: "small", LocalSize: 10, LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("small Upload used for a file that grew large: %d calls", len(c.uploads))
	}
	if !bytes.Equal(c.resumeChunks, content) {
		t.Errorf("streamed %d bytes via resume, want the %d live bytes", len(c.resumeChunks), live)
	}
	b, err := st.GetBaseline(7, "grew.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "gf" || b.LocalSize != live {
		t.Errorf("baseline = %+v, want synced gf with live size %d", b, live)
	}
}

// TestOutboundMkdirReplaySafeAfterLostReply models a mkdir whose success reply was lost on a
// prior attempt: the directory already exists server-side, so the retry's Mkdir returns
// ErrPathError. The postcondition QueryNode finds the directory and the op adopts it as a
// success rather than wedging forever.
func TestOutboundMkdirReplaySafeAfterLostReply(t *testing.T) {
	c := &pushClient{
		mkdirErr:  actions.ErrPathError,
		queryNode: map[string]actions.Node{"sub": {FileID: "d1", IsDir: true, MTime: 7}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub",
		LocalMTime: 100, UnixPerm: 755,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "sub")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "d1" {
		t.Errorf("baseline = %+v, want synced d1 adopted from the existing directory", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("mkdir op not acked after postcondition verification: %d remain", n)
	}
}

// TestOutboundMkdirPathErrorWithoutExistingDirRequeued confirms the replay-safety path does
// not mask a genuine failure: when the postcondition QueryNode finds no directory, the
// original ErrPathError surfaces and the op stays queued for retry.
func TestOutboundMkdirPathErrorWithoutExistingDirRequeued(t *testing.T) {
	c := &pushClient{mkdirErr: actions.ErrPathError} // queryNode returns a zero Node (not a dir)
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub", LocalMTime: 100,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the directory does not exist, got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("mkdir op not left queued: %d", n)
	}
}

// TestOutboundMkdirNoFileIDRecoveredViaQueryNode covers a successful Mkdir whose reply carried
// no file id: marking the baseline synced with an empty FileID would wedge every descendant
// create forever (resolveParentID → errParentUnknown) with the mkdir already acked. The op must
// recover the id by querying the freshly-created directory and record it.
func TestOutboundMkdirNoFileIDRecoveredViaQueryNode(t *testing.T) {
	c := &pushClient{
		// Mkdir succeeds but the reply has no file id (mkdirNode unset → zero Node).
		queryNode: map[string]actions.Node{"sub": {FileID: "d1", IsDir: true, MTime: 7}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub",
		LocalMTime: 100, UnixPerm: 755,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "sub")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "d1" {
		t.Errorf("baseline = %+v, want synced d1 recovered via QueryNode", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("mkdir op not acked after id recovery: %d remain", n)
	}
}

// TestOutboundMkdirNoFileIDRecoveryRejectsNonDir confirms the no-file-id recovery probe requires
// the recovered node to be a DIRECTORY, mirroring the ErrPathError replay branch. If a concurrent
// process replaced the path with a regular file between our mkdir and the probe, adopting that
// file's id as a directory baseline would strand every descendant op behind a non-directory
// parent - so a non-dir recovery is rejected and the op stays queued.
func TestOutboundMkdirNoFileIDRecoveryRejectsNonDir(t *testing.T) {
	c := &pushClient{
		// Mkdir succeeds but the reply has no file id; the probe finds a regular FILE at the path.
		queryNode: map[string]actions.Node{"sub": {FileID: "f1", IsDir: false, MTime: 7}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub", LocalMTime: 100,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when recovery finds a non-directory, got nil")
	}
	if _, err := st.GetBaseline(7, "sub"); err == nil {
		t.Error("baseline marked synced with a regular file adopted as a directory")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("mkdir op not left queued: %d", n)
	}
}

// TestOutboundMkdirNoFileIDUnrecoverableRequeued confirms that when a successful Mkdir reply
// carries no file id AND the directory id cannot be recovered, the op stays queued (an
// un-acked error) rather than acking a baseline with an unusable empty id that would strand
// descendants forever.
func TestOutboundMkdirNoFileIDUnrecoverableRequeued(t *testing.T) {
	c := &pushClient{} // Mkdir returns a zero Node (no id); queryNode empty → no recovery
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub", LocalMTime: 100,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the mkdir id is unknown and unrecoverable, got nil")
	}
	if _, err := st.GetBaseline(7, "sub"); err == nil {
		t.Error("baseline marked synced despite an unusable empty file id")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("mkdir op not left queued: %d", n)
	}
}

// midIOProbeClient decorates pushClient so a failing QueryNode marks the connection
// broken, modelling a recovery probe aborted mid-reply: the socket is left
// byte-misaligned, exactly the state actions.Client's poisoning discipline reports
// through Broken().
type midIOProbeClient struct {
	*pushClient
	mu     sync.Mutex
	broken bool
}

func (c *midIOProbeClient) QueryNode(ctx context.Context, viewID int64, p string) (actions.Node, error) {
	n, err := c.pushClient.QueryNode(ctx, viewID, p)
	if err != nil {
		c.mu.Lock()
		c.broken = true
		c.mu.Unlock()
	}
	return n, err
}

func (c *midIOProbeClient) Broken() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.broken
}

// TestOutboundMkdirIDRecoveryProbeBrokenRedialsWorker covers an error-swallow site: a successful
// Mkdir whose reply carried no file id, followed by a recovery probe (QueryNode) that dies mid-I/O
// and poisons the connection. Before the fix applyMkdirOut swallowed the probe error and returned
// nil, so WithWorker (which then consulted Broken() only on a non-nil callback error) released the
// live byte-misaligned socket back to the pool as healthy. Now the broken probe error
// propagates: the op stays queued for the next drain (transient, nothing
// dead-lettered, no baseline with an unusable empty id) and the poisoned worker is
// re-dialed through Replace.
func TestOutboundMkdirIDRecoveryProbeBrokenRedialsWorker(t *testing.T) {
	push := &pushClient{
		// Mkdir succeeds but echoes no file id (mkdirNode unset → zero Node); the
		// recovery probe then fails mid-reply.
		queryNodeErr: errors.New("read reply: connection reset mid-value"),
	}
	cli := &midIOProbeClient{pushClient: push}
	pool := &fakePool{}
	pt := newPoolTransport(pool, "tok", nil)
	pt.newClient = func(*transport.Session) syncClient { return cli }

	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e := New(st, pt, t.TempDir(), "sess", nil)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "sub", LocalMTime: 100,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the broken probe error to surface (transient), got nil")
	}
	if len(push.mkdirs) != 1 {
		t.Fatalf("mkdir calls = %+v, want 1", push.mkdirs)
	}
	// The poisoned worker was re-dialed, not returned to the pool as healthy.
	if pool.replaced != 1 {
		t.Errorf("pool.Replace calls = %d, want 1 (broken worker re-elected)", pool.replaced)
	}
	// The failure is transient: the op stays queued for the next drain, nothing was
	// dead-lettered, and no baseline was marked synced with an unusable empty id.
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (%v), want 1 (op stays queued)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (%v), want 0 (probe failure is transient)", n, err)
	}
	if _, err := st.GetBaseline(7, "sub"); err == nil {
		t.Error("baseline marked synced despite the failed id recovery")
	}
}

// TestOutboundRemoveIncompleteQueryNodeNotGone covers the trap that a clean QueryNode returning a
// present node WITHOUT a file_id (decodeNode tolerates an omitted id) must NOT be read as proof of
// deletion: the node is still on the server (e.g. the delete timed out), and acking the remove on
// that signal would drop the baseline while leaving the file remote forever. The parent listing is
// the sole arbiter, and here it sees the node still present, so the op stays queued.
func TestOutboundRemoveIncompleteQueryNodeNotGone(t *testing.T) {
	c := &pushClient{
		removeErr: actions.ErrPathError,
		// A QueryNode probe on the path would return a present node lacking a file_id - the
		// incomplete-metadata trap. It must not be consulted as proof of absence.
		queryNode: map[string]actions.Node{"gone.txt": {Name: "gone.txt"}},
		// The parent listing shows the node is still present at the leaf: not gone.
		listResult: map[string][]actions.Node{"": {{Name: "gone.txt", FileID: "f2"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the node is still present, got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped despite the node still being present: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveRenamedNodeInconclusive covers a Remove that returned ErrPathError where a
// concurrent remote rename has removed the leaf NAME but the node lives on under a new name in
// the same parent. Confirming absence by missing leaf name alone would wrongly drop the
// baseline and ack a delete that may never have executed; keying on the node's file id sees it
// still present, so the result is inconclusive and the op stays queued.
func TestOutboundRemoveRenamedNodeInconclusive(t *testing.T) {
	c := &pushClient{
		removeErr:    actions.ErrPathError,
		queryNodeErr: actions.ErrPathError,
		// The leaf "gone.txt" is absent, but the same node (FileID f2) is present as "renamed.txt".
		listResult: map[string][]actions.Node{"": {{Name: "renamed.txt", FileID: "f2"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the node is still present under a new name, got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped though the node (by id) is still present: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveBareEmptyListingStaysQueued is the engine mirror of the actions-layer
// TestListRejectsBareEmptyReply: a server answering the remove
// postcondition's parent-listing probe with a bare {} - no errno, no file_list - now surfaces
// from actions.List as ErrUnexpectedReply, which the absence probe treats as inconclusive
// (ancestorConfirmsAbsent only ever walks on ErrPathError), so the remove op stays queued and
// its baseline survives. Before the fix the same wire reply decoded as (nil, nil) - an
// authoritative empty listing - and confirmAbsentByParentList took it as positive proof of
// absence, falsely acking a remove the server never performed (a silently lost local delete).
func TestOutboundRemoveBareEmptyListingStaysQueued(t *testing.T) {
	c := &pushClient{
		removeErr: actions.ErrPathError,
		// What actions.List returns for a bare {} reply since errno-less replies are rejected;
		// before that
		// this was (nil, nil), which read as a clean empty listing → confirmed gone.
		listErr: actions.ErrUnexpectedReply,
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error on a contentless listing probe (inconclusive), got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped on a contentless listing probe: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveUnevidencedListingStaysQueued pins the unevidenced-listing fix one step past
// the bare-{} case above. Here the reply IS evidenced as a turn - it carries errno:0 - but it
// carries no file_list at all, so nothing was enumerated. actions.List reports that as
// (nil, listed=false, nil): a successful turn with no listing. Before the fix the caller could
// not see the difference and iterated the zero-length slice as an authoritative empty parent -
// positive proof the node was gone - acking a remove the server never performed and dropping
// the baseline: the local delete silently lost, the server file alive. resolveServerNodeAbsence
// now requires listed, so the probe is inconclusive: the op stays queued and the baseline lives.
func TestOutboundRemoveUnevidencedListingStaysQueued(t *testing.T) {
	c := &pushClient{
		removeErr: actions.ErrPathError,
		// errno:0 with no file_list - a successful turn that enumerated nothing.
		listUnevidenced: map[string]bool{"": true},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error on an unenumerated listing (inconclusive), got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped on an unenumerated listing: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveEnumeratedEmptyParentStillAcks is the positive control for the test above:
// a reply carrying an actual (empty) file_list - a genuinely empty parent directory - is a real
// enumeration, so it stays what it always was, positive proof the node is gone. The remove acks
// idempotently and the baseline is dropped. The fix must tighten only the unenumerated
// case; tightening this one too would wedge every delete under a now-empty directory.
func TestOutboundRemoveEnumeratedEmptyParentStillAcks(t *testing.T) {
	c := &pushClient{
		removeErr: actions.ErrPathError,
		// file_list: [] - the parent listed cleanly and holds nothing.
		listResult: map[string][]actions.Node{"": {}},
		// ...and the corroborating probe of the node's own path answers with the
		// path-error sentinel, which is what a genuinely deleted node looks like.
		queryNodeErrByPath: map[string]error{"gone.txt": actions.ErrPathError},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (an enumerated empty parent is proof of absence)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued: %d, want acked on a confirmed-absent node", n)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err == nil {
		t.Error("baseline survived a remove whose absence the empty listing confirmed")
	}
}

// TestOutboundRemoveUnidentifiableChildInconclusive covers a parent listing that includes a
// child with no name and no file id (decodeNode tolerates both). Such a child could be the very
// node being removed, so absence cannot be proven past it and the result is inconclusive - the
// op stays queued rather than being acked on a false "gone".
func TestOutboundRemoveUnidentifiableChildInconclusive(t *testing.T) {
	c := &pushClient{
		removeErr:    actions.ErrPathError,
		queryNodeErr: actions.ErrPathError,
		listResult:   map[string][]actions.Node{"": {{Name: "", FileID: ""}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error on an unidentifiable child (inconclusive), got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped on an inconclusive listing: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveDeletedParentConfirmedAbsentUpward reproduces a queue
// wedge and pins the symptomatic fix: an already-queued outbound OpRemove for a child whose
// whole parent chain was deleted server-side. Remove fails with ErrPathError and every listing
// of the deleted ancestors ("a/b", then "a") fails with ErrPathError too, so before the ancestor
// walk the absence stayed "inconclusive" and the op retried forever. The walk reaches the view
// root, which lists cleanly and omits "a" - positive proof the chain (and the child with it) is
// gone - so the op converges (acked, baseline dropped) in a single drain.
func TestOutboundRemoveDeletedParentConfirmedAbsentUpward(t *testing.T) {
	c := &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"a/b": actions.ErrPathError, "a": actions.ErrPathError},
		listResult:    map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fx"}}},
		// Every deleted path answers the path-error sentinel, so each rung of the walk - and the
		// node itself - corroborates its listing-derived verdict.
		queryNodeErrByPath: map[string]error{
			"a": actions.ErrPathError, "a/b": actions.ErrPathError, "a/b/child.txt": actions.ErrPathError,
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "a/b/child.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/b/child.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the remove to converge in one drain)", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op still queued: %d, want acked via the ancestor walk", n)
	}
	if _, err := st.GetBaseline(7, "a/b/child.txt"); err == nil {
		t.Error("baseline survived a remove whose absence the ancestor walk confirmed")
	}
	// The walk listed the parent, then each ancestor, terminating at the clean view root - once
	// per drain pass, since the absence corroboration needs agreeing probes across passes before
	// each rung's verdict is actionable and every pass re-runs the same walk.
	// The walk terminates at the view root, addressed as "/" (viewRootPath).
	wantLists := []string{"a/b", "a", viewRootPath}
	if len(c.listCalls) == 0 || len(c.listCalls)%len(wantLists) != 0 {
		t.Fatalf("list calls = %q, want whole repetitions of %q", c.listCalls, wantLists)
	}
	for i, got := range c.listCalls {
		if got != wantLists[i%len(wantLists)] {
			t.Fatalf("list calls = %q, want repetitions of %q", c.listCalls, wantLists)
		}
	}
}

// TestOutboundRemoveParentListTimeoutStillInconclusive is the conservative side of the ancestor
// walk: List(parent) fails with ErrPathError but the view root still SHOWS the parent - the
// listing failure was a server-side timeout, not a delete. The child's absence stays unproven,
// so the op stays queued and the baseline survives (no false ack on a transient blip).
func TestOutboundRemoveParentListTimeoutStillInconclusive(t *testing.T) {
	c := &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"d": actions.ErrPathError},
		listResult:    map[string][]actions.Node{"": {{Name: "d", FileID: "fd"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "d/child.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "d/child.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error (parent still present, absence inconclusive), got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
	if _, err := st.GetBaseline(7, "d/child.txt"); err != nil {
		t.Errorf("baseline dropped on an inconclusive ancestor walk: %v", err)
	}
}

// TestOutboundRemoveRenamedParentReEmitsNotFalselyAcked reproduces the renamed-parent false ack
// and pins the fix. Baseline a/b/c is synced; the user deletes c locally (an outbound
// OpRemove(a/b/c), FileID fid-c), and the server renames the directory a/b -> a/x, whose inbound
// event has already reconciled - relocating the node's baseline to a/x/c (StatePending) and
// re-indexing fid-c there. On drain Remove(a/b/c) fails ErrPathError and every listing of the
// now-renamed a/b errors too, so the ancestor walk lists a cleanly, sees "x" but not "b", and reads
// "a/b gone" - falsely, since the subtree lives on under a/x. Before the fix that acked the remove
// as an idempotent success: the delete never ran (the file survives on the server) and the
// relocated a/x/c baseline sat StatePending forever, inert (localReconcile only drives
// StateSynced). The fix cross-checks PathByID(fid-c): re-indexed at a/x/c != op.Path, so the stale
// remove is re-emitted - the baseline is reverted to StateSynced and the next scan pushes an
// OpRemove(a/x/c) against the new path.
func TestOutboundRemoveRenamedParentReEmitsNotFalselyAcked(t *testing.T) {
	c := &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"a/b": actions.ErrPathError},
		// The rename already reconciled: listing a shows the directory under its NEW name x
		// (matching the relocated a/x/c baseline), and omits the old b entirely.
		listResult: map[string][]actions.Node{"a": {{Name: "x", FileID: "fid-x"}}},
		// The OLD paths resolve to nothing server-side (the subtree lives under a/x now), so the
		// corroborating probes agree with the listing - the walk still reads "a/b gone", falsely,
		// and the PathByID cross-check remains the thing that saves the delete (the listing
		// evidence rule hardens the listing, it cannot see through a rename).
		queryNodeErrByPath: map[string]error{"a/b": actions.ErrPathError, "a/b/c": actions.ErrPathError},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// Seed the tree as the inbound rename left it: a/ and a/x/ are tracked dirs, a/x/c is the
	// relocated node (StatePending, indexed by fid-c), and a sibling a/x/keep.txt keeps the view
	// non-empty so the next scan is not swallowed by the empty-view debounce.
	keep := []byte("keep")
	keepHash := md4.ContentHash(keep)
	if err := os.MkdirAll(filepath.Join(viewDir, "a", "x"), 0o755); err != nil {
		t.Fatalf("seed dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x", "keep.txt"), keep, 0o644); err != nil {
		t.Fatalf("seed keep file: %v", err)
	}
	seed := []struct {
		path string
		b    store.Baseline
	}{
		{"a", store.Baseline{FileID: "fid-a", FileType: "dir", State: store.StateSynced}},
		{"a/x", store.Baseline{FileID: "fid-x", FileType: "dir", State: store.StateSynced}},
		{"a/x/c", store.Baseline{FileID: "fid-c", FileType: "file", LocalHash: "hc", LocalSize: 2, State: store.StatePending}},
		{"a/x/keep.txt", store.Baseline{FileID: "fid-k", FileType: "file", LocalHash: keepHash, LocalSize: int64(len(keep)), State: store.StateSynced}},
	}
	for _, s := range seed {
		if err := st.PutBaseline(7, s.path, s.b); err != nil {
			t.Fatalf("seed baseline %q: %v", s.path, err)
		}
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/b/c", FileID: "fid-c",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	// The stale op left the queue (re-emit resolution), but the node was NOT deleted server-side -
	// Remove was attempted only against the old path and failed.
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("stale remove op still queued: %d, want re-emit resolution", n)
	}
	// Every attempt addressed the OLD path and failed; nothing was ever deleted at the node's live
	// path. (One attempt per drain pass: the absence corroboration needs agreeing probes across
	// passes before the walk's verdict is actionable.)
	if len(c.removes) == 0 {
		t.Errorf("removes = %q, want the failed attempt(s) against the old path a/b/c", c.removes)
	}
	for _, r := range c.removes {
		if r != "a/b/c" {
			t.Errorf("removes = %q, want every attempt against the old path a/b/c only", c.removes)
		}
	}
	// The relocated baseline is reverted to StateSynced so the next scan can re-detect the deletion.
	b, err := st.GetBaseline(7, "a/x/c")
	if err != nil {
		t.Fatalf("GetBaseline a/x/c: %v (baseline must survive - the delete never ran)", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("a/x/c state = %v, want StateSynced (reverted so the next scan re-emits)", b.State)
	}
	if b.FileID != "fid-c" {
		t.Errorf("a/x/c FileID = %q, want fid-c preserved", b.FileID)
	}

	// The next local scan re-detects the still-deleted-on-disk file and re-emits the remove against
	// the node's CURRENT path a/x/c (carrying its file id), so the delete finally reaches the server.
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	var found bool
	for _, op := range ops {
		if op.Type == store.OpRemove && op.Path == "a/x/c" {
			if op.FileID != "fid-c" {
				t.Errorf("re-emitted OpRemove FileID = %q, want fid-c", op.FileID)
			}
			found = true
		}
		if op.Type == store.OpRemove && op.Path == "a/b/c" {
			t.Errorf("re-emitted an OpRemove against the stale old path a/b/c: %+v", op)
		}
	}
	if !found {
		t.Errorf("next scan did not re-emit OpRemove(a/x/c); ops = %+v", ops)
	}
}

// TestOutboundRemoveGenuinelyDeletedParentStillAcks is the renamed-parent fix's regression guard:
// the ancestor walk still converges a truly-deleted subtree. Here fid-2 is NOT relocated - its byid
// index still points at op.Path (a genuine local delete, planRemove flips the baseline pending but
// leaves byid alone) - so the PathByID cross-check finds cur == op.Path and takes the
// idempotent-absence ack, exactly as before the renamed-parent change (mirroring
// TestOutboundRemoveDeletedParentConfirmedAbsentUpward).
func TestOutboundRemoveGenuinelyDeletedParentStillAcks(t *testing.T) {
	c := &pushClient{
		removeErr:     actions.ErrPathError,
		listErrByPath: map[string]error{"a/b": actions.ErrPathError, "a": actions.ErrPathError},
		listResult:    map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fx"}}},
		// The subtree is genuinely deleted, so every path in it answers the path-error sentinel
		// and each rung of the walk corroborates.
		queryNodeErrByPath: map[string]error{
			"a": actions.ErrPathError, "a/b": actions.ErrPathError, "a/b/child.txt": actions.ErrPathError,
		},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	// byid points fid-2 -> a/b/child.txt (== op.Path): a genuine delete, no relocation.
	if err := st.PutBaseline(7, "a/b/child.txt", store.Baseline{FileID: "fid-2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "a/b/child.txt", FileID: "fid-2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("genuine remove still queued: %d, want acked via the ancestor walk", n)
	}
	if _, err := st.GetBaseline(7, "a/b/child.txt"); err == nil {
		t.Error("baseline survived a genuinely-confirmed remove; want dropped")
	}
}

// TestOutboundRenameReplaySafeAfterLostReply models a rename whose success reply was lost:
// the retry's Rename fails with ErrPathError (the source is already gone), but the
// postcondition QueryNode finds the moved node at the destination, so the op succeeds.
func TestOutboundRenameReplaySafeAfterLostReply(t *testing.T) {
	c := &pushClient{
		renameErr: actions.ErrPathError,
		queryNode: map[string]actions.Node{"new.txt": {FileID: "f9"}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "new.txt", store.Baseline{FileID: "f9", FileType: "file", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "old.txt", NewPath: "new.txt", FileID: "f9", LocalHash: "h9", LocalSize: 4,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	b, err := st.GetBaseline(7, "new.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "f9" {
		t.Errorf("baseline = %+v, want synced f9 (move confirmed at destination)", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("rename op not acked after postcondition verification: %d remain", n)
	}
}

func TestOutboundResumeNoCompletionRequeued(t *testing.T) {
	// The whole large file is streamed but the server never reports completion: the op must
	// stay queued (returning an error) rather than ack a baseline marked synced with an
	// all-zero node.
	c := &pushClient{
		resumeToken:      actions.ResumeToken{Token: "tk"},
		resumeNoComplete: true,
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	size := uploadResumeThreshold + 10
	content := make([]byte, size)
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: "bh", LocalSize: int64(size), LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error for no-completion resume, got nil")
	}
	// The whole file was streamed, but no synced baseline was recorded.
	if int64(len(c.resumeChunks)) != int64(size) {
		t.Errorf("streamed %d bytes, want %d", len(c.resumeChunks), size)
	}
	if _, err := st.GetBaseline(7, "big.bin"); err == nil {
		t.Error("baseline recorded despite no completing reply")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d", n)
	}
}

// TestOutboundUploadResumePartialCommitReseeks models a server that commits fewer bytes than
// the chunk carried (a partial commit), advancing its expected offset to a point inside the
// chunk just sent. The client must re-seek the file to that offset before reading the next
// chunk; without the re-seek the descriptor stays ahead of the offset and streams the wrong
// bytes under each offset, corrupting the assembled file.
func TestOutboundUploadResumePartialCommitReseeks(t *testing.T) {
	c := &pushClient{
		resumeToken: actions.ResumeToken{Token: "tk"},
		resumeNode:  actions.Node{FileID: "bigf", Hash: "bh", MTime: 9},
		// First chunk: the server commits only the first 512 KiB of the 1 MiB sent, so the
		// client must rewind to 512 KiB and re-send from there. Later chunks commit fully.
		resumeCommitOffsets: []int64{512 << 10},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	size := uploadResumeThreshold + 4096 // several full chunks plus a tail
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: md4.ContentHash(content), LocalSize: int64(size), LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The bytes the server assembled at their declared offsets must reconstruct the file
	// exactly: every offset carried the content that belongs there.
	if !bytes.Equal(c.resumeAssembled, content) {
		t.Errorf("assembled %d bytes did not match the %d-byte file: the reader desynced from the offset after a partial commit", len(c.resumeAssembled), size)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "bigf" {
		t.Errorf("baseline = %+v, want synced bigf", b)
	}
}

// TestOutboundRemoveTimeoutNodeStillPresentRequeued covers a Remove that returns ErrPathError
// because the operation timed out server-side rather than because the node was already gone.
// The postcondition probe finds the node still present (its parent listing still carries it),
// so the op must stay queued (its baseline surviving) rather than being acked as a completed
// delete.
func TestOutboundRemoveTimeoutNodeStillPresentRequeued(t *testing.T) {
	c := &pushClient{
		removeErr: actions.ErrPathError,
		// The node is still on the server: the delete never executed (a timeout). Both the
		// direct probe and the parent listing still report it present.
		queryNode:  map[string]actions.Node{"gone.txt": {FileID: "f2"}},
		listResult: map[string][]actions.Node{"": {{Name: "gone.txt", FileID: "f2"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error for a remove that never executed, got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped despite the node still being present: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveProbePathErrorRequeued covers the double-timeout case: the Remove, the
// postcondition QueryNode probe, AND the parent listing all error. Because ErrPathError
// conflates "node absent" with "operation timed out", a probe ErrPathError is NOT proof the
// node is gone, and a failed parent listing is inconclusive too - a server timing out every
// call would otherwise drop a baseline while the remote node remains. The op must stay queued
// for retry rather than being acked as a completed delete.
func TestOutboundRemoveProbePathErrorRequeued(t *testing.T) {
	c := &pushClient{
		removeErr:    actions.ErrPathError,
		queryNodeErr: actions.ErrPathError, // the probe also errors out (ambiguous: absent or timeout)
		listErr:      actions.ErrPathError, // and the parent listing is inconclusive (a timeout too)
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the deletion is unconfirmed, got nil")
	}
	if _, err := st.GetBaseline(7, "gone.txt"); err != nil {
		t.Errorf("baseline dropped on an unconfirmed (ambiguous) deletion: %v", err)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("remove op not left queued for retry: %d", n)
	}
}

// TestOutboundRemoveLostReplyConfirmedByParentList covers a Remove that actually completed
// server-side but whose success reply was lost, so the retry sees ErrPathError (node already
// gone). The direct QueryNode probe is itself ErrPathError (a production server reports an
// absent node that way), which is indistinguishable from a timeout - but listing the parent
// directory succeeds and the leaf is absent, which is unambiguous proof the node is gone. The
// op must be acked and its baseline dropped, never wedged forever (completed removes used to
// stick because QueryNode can't confirm absence).
func TestOutboundRemoveLostReplyConfirmedByParentList(t *testing.T) {
	c := &pushClient{
		removeErr:    actions.ErrPathError, // the node is already gone (the first delete succeeded)
		queryNodeErr: actions.ErrPathError, // QueryNode on an absent node also reports ErrPathError
		// The parent listing succeeds and omits the leaf: positive proof of absence.
		listResult: map[string][]actions.Node{"": {{Name: "other.txt", FileID: "f9"}}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "gone.txt", store.Baseline{FileID: "f2", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirOutbound, ViewID: 7, Path: "gone.txt", FileID: "f2",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: a completed remove confirmed gone must ack, got %v", err)
	}
	if _, err := st.GetBaseline(7, "gone.txt"); !isNotFound(err) {
		t.Errorf("baseline not dropped after a confirmed-gone remove: err=%v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("remove op left queued despite confirmed absence: %d", n)
	}
	if len(c.listCalls) == 0 {
		t.Error("parent listing was never consulted to confirm absence")
	}
}

// TestOutboundReadOnlyViewDropsQueuedOp covers an op enqueued while a view was writable whose
// privilege has since downgraded to read-only. Draining it must not push (the server would
// reject it as unauthorized on every retry); the op is dropped and its baseline marked
// StateConflict instead.
func TestOutboundReadOnlyViewDropsQueuedOp(t *testing.T) {
	c := &pushClient{}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", Privilege: "ro", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Photos"), 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileType: "file", LocalHash: "h1", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", LocalHash: "h1",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 0 {
		t.Errorf("pushed %d uploads to a read-only view, want 0", len(c.uploads))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("read-only op not dropped from the queue: %d", n)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	// The read-only drop must leave a durable dead-letter trace so status reports it.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped read-only op", n, err)
	}
}

// leaseSeqTransport hands out a different client per WithWorker lease, in declared order
// (the last client repeats once exhausted). It lets a test model the upload's own connection
// being poisoned - its QueryNode fails - while a SEPARATE, fresh lease recovers, proving the
// small-upload lost-reply probe does not run on the broken connection.
type leaseSeqTransport struct {
	clients []syncClient
	n       int
}

func (t *leaseSeqTransport) EventClient() (syncClient, error) { return t.clients[0], nil }

func (t *leaseSeqTransport) WithWorker(_ context.Context, fn func(syncClient) error) error {
	c := t.clients[t.n]
	if t.n < len(t.clients)-1 {
		t.n++
	}
	return fn(c)
}

func (t *leaseSeqTransport) ReplaceEvent(context.Context) error { return nil }

// TestOutboundUploadSmallLostReplyProbeUsesFreshLease confirms the small-upload lost-reply
// recovery runs on a SEPARATE worker lease, not the upload's own connection. A real small
// Upload poisons its connection on every server rejection, so a QueryNode there returns
// ErrConnBroken; the recovery would be dead code if it reused that connection. Here the first
// lease (the upload) returns ErrPathError and its QueryNode is broken, while the fresh probe
// lease finds the node carrying our hash and adopts it as success.
func TestOutboundUploadSmallLostReplyProbeUsesFreshLease(t *testing.T) {
	content := []byte("hello")
	wantHash := md4.ContentHash(content)
	uploadConn := &pushClient{
		uploadErr: actions.ErrPathError,
		// The pre-turn divergence probe (finding H1) runs on this healthy connection and sees the
		// node our own lost-reply attempt committed - same content, so the push proceeds. Only
		// AFTER the Upload is the connection poisoned, which is what the recovery must not reuse.
		queryNode:        map[string]actions.Node{"a.txt": {FileID: "existf", Hash: wantHash, MTime: 55}},
		queryNodeOKFirst: 1,
		queryNodeErr:     actions.ErrConnBroken, // the poisoned upload connection cannot probe
	}
	probeConn := &pushClient{
		queryNode: map[string]actions.Node{"a.txt": {FileID: "existf", Hash: wantHash, MTime: 55}},
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
		FileType: "file", LocalHash: wantHash, LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: wantHash, LocalSize: int64(len(content)), UnixPerm: 644,
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
	if b.State != store.StateSynced || b.FileID != "existf" {
		t.Errorf("baseline = %+v, want synced existf recovered via a fresh probe lease", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("op not acked after fresh-lease recovery: %d remain", n)
	}
}

// TestOutboundUploadSmallCreateNoFileIDRecovered covers a successful FRESH upload whose reply
// omitted a file id. Marking the baseline synced with an empty id would drop the node from the
// byid index, so the op recovers the id by querying the just-uploaded file.
func TestOutboundUploadSmallCreateNoFileIDRecovered(t *testing.T) {
	content := []byte("hello")
	wantHash := md4.ContentHash(content)
	c := &pushClient{
		// Upload succeeds but the reply carries no file id (uploadNode unset → zero Node).
		queryNode: map[string]actions.Node{"a.txt": {FileID: "recf", Hash: wantHash, MTime: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: wantHash, LocalSize: int64(len(content)), UnixPerm: 644,
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
	if b.State != store.StateSynced || b.FileID != "recf" {
		t.Errorf("baseline = %+v, want synced recf recovered via QueryNode", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("op not acked after id recovery: %d remain", n)
	}
}

// TestOutboundUploadSmallCreateNoFileIDUnrecoverableRequeued confirms a fresh upload whose reply
// omits a file id AND whose recovery probe also yields no id leaves the op QUEUED rather than
// acking an unindexed baseline that would drop the node from byid.
func TestOutboundUploadSmallCreateNoFileIDUnrecoverableRequeued(t *testing.T) {
	content := []byte("hello")
	c := &pushClient{} // Upload returns a zero Node (no id); queryNode empty → no recovery
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error when the upload id is unknown and unrecoverable, got nil")
	}
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Error("baseline marked synced despite an unusable empty file id")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("upload op not left queued: %d", n)
	}
}

// TestOutboundUploadModifyNoFileIDKeepsExistingID confirms a MODIFY whose reply omits a file id
// keeps the existing op.FileID (no recovery probe needed) rather than being treated as an
// unrecoverable create.
func TestOutboundUploadModifyNoFileIDKeepsExistingID(t *testing.T) {
	content := []byte("changed")
	c := &pushClient{} // Upload returns a zero Node (no id echoed on a modify)
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "existing", FileType: "file", LocalHash: "old", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "existing", LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), UnixPerm: 644,
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
	if b.State != store.StateSynced || b.FileID != "existing" {
		t.Errorf("baseline = %+v, want synced keeping the existing id", b)
	}
	if len(c.uploads) != 1 {
		t.Errorf("uploads = %d, want 1", len(c.uploads))
	}
}
