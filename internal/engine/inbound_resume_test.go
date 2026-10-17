package engine

// These tests cover the Phase-G inbound resume path: applyDownload streams a file larger than
// the resume threshold in offset-based resume_download chunks (never buffering it whole),
// resumes from the next offset across server-chosen chunk boundaries, verifies the whole-file
// MD4 of the assembled result, and still promotes via fsync+rename. Small files keep the
// single-frame Download. A chunk error aborts the transfer and the op re-runs from scratch on
// the next drain.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// resumeDLClient is a syncClient that also implements resumeDownloadClient. Its ResumeDownload
// serves content[fileID][offset:offset+chunkSize], modelling an offset-addressed server that
// chooses each chunk's length; it records the offsets requested and the call counts so a test
// can assert the resume loop drove successive offsets and never fell back to single-frame
// Download. failOnceAtOffset, when >= 0, makes the first ResumeDownload at that offset return an
// error once (a mid-transfer interruption) before succeeding on a later attempt.
type resumeDLClient struct {
	mu sync.Mutex

	content map[string][]byte
	mtime   int64

	// chunkSize bounds how many bytes each ResumeDownload returns (the server's choice); 0
	// means "the whole remainder in one chunk".
	chunkSize int64

	failOnceAtOffset int64
	failed           bool

	// queryNode/queryErr script the fresh-facts QueryNode the resume apply issues before
	// streaming; the zero values model a tolerant empty reply, which
	// falls back to the event snapshot.
	queryNode actions.Node
	queryErr  error

	downloadCalls int
	tokenCalls    int
	resumeCalls   int
	queryCalls    int
	queryPaths    []string
	offsets       []int64
}

func (c *resumeDLClient) QueryUserInfo(context.Context) ([]actions.View, error) { return nil, nil }
func (c *resumeDLClient) QueryNode(_ context.Context, _ int64, path string) (actions.Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCalls++
	c.queryPaths = append(c.queryPaths, path)
	if c.queryErr != nil {
		return actions.Node{}, c.queryErr
	}
	return c.queryNode, nil
}

// TestResumeDownloadQueriesCurrentTrackedPath covers a queued download whose
// enqueue-time path became stale after a server-side move. Fresh-facts lookup
// must follow FileID through the baseline index or the resume verifier can use
// stale size/hash facts forever.
func TestResumeDownloadQueriesCurrentTrackedPath(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutBaseline(7, "new.bin", store.Baseline{FileID: "big", FileType: "file"}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	body := []byte("current server content")
	c := &resumeDLClient{
		content:          map[string][]byte{"big": body},
		queryNode:        actions.Node{FileID: "big", Hash: md4.ContentHash(body), Size: int64(len(body))},
		failOnceAtOffset: -1,
	}
	e := New(st, &concTransport{client: c}, t.TempDir(), "sess", nil)
	var dst bytes.Buffer
	if _, err := e.resumeDownloadToTemp(context.Background(), store.Op{
		ViewID: 7, Path: "old.bin", FileID: "big", LocalHash: "stale", LocalSize: 999,
	}, &dst); err != nil {
		t.Fatalf("resumeDownloadToTemp: %v", err)
	}
	if len(c.queryPaths) != 1 || c.queryPaths[0] != "new.bin" {
		t.Fatalf("QueryNode paths = %v, want [new.bin]", c.queryPaths)
	}
	if !bytes.Equal(dst.Bytes(), body) {
		t.Fatalf("downloaded content = %q, want %q", dst.Bytes(), body)
	}
}
func (c *resumeDLClient) LongPoll(context.Context, []actions.Subscription, int) ([]int64, error) {
	return nil, nil
}
func (c *resumeDLClient) PullEvent(context.Context, int64, int64, int64) ([]actions.Event, int64, error) {
	return nil, 0, nil
}

func (c *resumeDLClient) Download(_ context.Context, fileID string, _, _ int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.downloadCalls++
	c.mu.Unlock()
	body := c.content[fileID]
	if _, err := w.Write(body); err != nil {
		return actions.Node{}, err
	}
	return actions.Node{FileID: fileID, Hash: md4.ContentHash(body), Size: int64(len(body)), MTime: c.mtime}, nil
}

func (c *resumeDLClient) ResumeGetToken(_ context.Context, req actions.ResumeRequest) (actions.ResumeToken, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokenCalls++
	return actions.ResumeToken{Token: fmt.Sprintf("tok-view-%d", req.ViewID)}, nil
}

func (c *resumeDLClient) ResumeDownload(_ context.Context, _, fileID string, _, _, offset int64, w io.Writer) (actions.Node, error) {
	c.mu.Lock()
	c.resumeCalls++
	c.offsets = append(c.offsets, offset)
	if c.failOnceAtOffset >= 0 && offset == c.failOnceAtOffset && !c.failed {
		c.failed = true
		c.mu.Unlock()
		return actions.Node{}, errors.New("simulated mid-transfer interruption")
	}
	c.mu.Unlock()

	body := c.content[fileID]
	end := int64(len(body))
	if c.chunkSize > 0 && offset+c.chunkSize < end {
		end = offset + c.chunkSize
	}
	chunk := body[offset:end]
	if _, err := w.Write(chunk); err != nil {
		return actions.Node{}, err
	}
	return actions.Node{FileID: fileID, Hash: md4.ContentHash(chunk), Size: int64(len(chunk)), MTime: c.mtime}, nil
}

// TestInboundDownloadLargeFileUsesResume confirms a file over the resume threshold is fetched
// in offset-based resume_download chunks (successive offsets, no single-frame Download), the
// assembled content matches, and the baseline records the whole-file facts.
func TestInboundDownloadLargeFileUsesResume(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	content := []byte("the quick brown fox jumps over the lazy dog, twice over")
	c := &resumeDLClient{
		content:          map[string][]byte{"big": content},
		mtime:            1700000123,
		chunkSize:        16,
		failOnceAtOffset: -1,
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	e.limits.UploadResumeThreshold = 16 // force the resume path for this small test file

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "docs/big.bin",
		FileID: "big", LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)),
		LocalMTime: 1700000123, UnixPerm: 644, ServerSyncID: 9,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(root, "Photos", "docs", "big.bin"))
	if err != nil {
		t.Fatalf("read applied file: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("content = %q, want %q", got, content)
	}
	if c.downloadCalls != 0 {
		t.Errorf("single-frame Download called %d times, want 0 (resume path)", c.downloadCalls)
	}
	wantChunks := (len(content) + 15) / 16
	if c.resumeCalls != wantChunks {
		t.Errorf("ResumeDownload called %d times, want %d", c.resumeCalls, wantChunks)
	}
	// The offsets requested must be strictly increasing and start at 0 (resume-from-offset).
	if len(c.offsets) == 0 || c.offsets[0] != 0 {
		t.Fatalf("offsets = %v, want first 0", c.offsets)
	}
	for i := 1; i < len(c.offsets); i++ {
		if c.offsets[i] <= c.offsets[i-1] {
			t.Fatalf("offsets not strictly increasing: %v", c.offsets)
		}
	}

	b, err := st.GetBaseline(7, "docs/big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("state = %q, want synced", b.State)
	}
	if b.LocalHash != md4.ContentHash(content) || b.ServerHash != md4.ContentHash(content) {
		t.Errorf("baseline hashes = %q/%q, want %q", b.LocalHash, b.ServerHash, md4.ContentHash(content))
	}
	if b.LocalSize != int64(len(content)) {
		t.Errorf("baseline size = %d, want %d", b.LocalSize, len(content))
	}
	if b.ServerSyncID != 9 {
		t.Errorf("baseline sync id = %d, want 9", b.ServerSyncID)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundDownloadSmallFileUsesSingleTurn confirms a file at or below the threshold keeps the
// single-frame Download and never enters the resume loop.
func TestInboundDownloadSmallFileUsesSingleTurn(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	content := []byte("small")
	c := &resumeDLClient{content: map[string][]byte{"f1": content}, mtime: 42, failOnceAtOffset: -1}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	e.limits.UploadResumeThreshold = 16

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		FileID: "f1", LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if c.resumeCalls != 0 || c.tokenCalls != 0 {
		t.Errorf("resume path used for a small file: resumeCalls=%d tokenCalls=%d", c.resumeCalls, c.tokenCalls)
	}
	if c.downloadCalls != 1 {
		t.Errorf("single-frame Download called %d times, want 1", c.downloadCalls)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "a.txt"))
	if err != nil || string(got) != string(content) {
		t.Fatalf("a.txt = %q (err %v), want %q", got, err, content)
	}
}

// TestInboundDownloadResumeAfterInterruption confirms a chunk error aborts the transfer (the op
// stays queued and the staging temp is cleaned), and a second drain completes the download from
// a fresh start - the cross-drain resume the design provides (no partial temp persisted).
func TestInboundDownloadResumeAfterInterruption(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	content := []byte("interruption-resume content spanning several resume chunks here")
	c := &resumeDLClient{
		content:          map[string][]byte{"big": content},
		mtime:            7,
		chunkSize:        16,
		failOnceAtOffset: 16, // fail the second chunk once
	}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	e.limits.UploadResumeThreshold = 16

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "big.bin",
		FileID: "big", LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// First drain hits the simulated interruption: it errors and leaves the op queued.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("first drainQueue succeeded, want the simulated interruption error")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("op not left queued after interruption: %d pending", n)
	}
	// No partial file is promoted onto the target path while the transfer is incomplete.
	if _, err := os.Stat(filepath.Join(root, "Photos", "big.bin")); !os.IsNotExist(err) {
		t.Errorf("partial file promoted before completion: %v", err)
	}

	// Second drain completes the download.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("second drainQueue: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "big.bin"))
	if err != nil || string(got) != string(content) {
		t.Fatalf("big.bin = %q (err %v), want %q", got, err, content)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestInboundDownloadResumeHashMismatch confirms the whole-file MD4 check on the assembled result
// rejects content that does not match the event's advertised hash, leaving nothing promoted.
func TestInboundDownloadResumeHashMismatch(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	content := []byte("bytes whose advertised hash will not match the assembled file")
	c := &resumeDLClient{content: map[string][]byte{"big": content}, chunkSize: 16, failOnceAtOffset: -1}
	e := New(st, &concTransport{client: c}, root, "sess", nil)
	e.limits.UploadResumeThreshold = 16

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "big.bin",
		FileID: "big", LocalHash: md4.ContentHash([]byte("a different file entirely")),
		LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drainQueue succeeded, want a hash-mismatch error")
	}
	if _, serr := os.Stat(filepath.Join(root, "Photos", "big.bin")); !os.IsNotExist(serr) {
		t.Errorf("file promoted despite hash mismatch: %v", serr)
	}
}

// TestInboundDownloadResumeStaleEventConverges reproduces the stale-event resume bug: a file
// over the resume threshold is modified AGAIN server-side between the event and the apply, so
// the op's hash/size snapshot is stale. Verifying the current bytes against the old hash (or
// streaming the old byte count of shrunk content) failed every drain - a full download per
// cycle, never converging, wedging the op and the conflict chain behind it. With the fresh
// QueryNode facts the apply streams and verifies the CURRENT content and converges in one
// drain, recording the actual facts on the synced baseline.
func TestInboundDownloadResumeStaleEventConverges(t *testing.T) {
	oldContent := []byte("the ORIGINAL server content the event described, spanning chunks")
	cases := []struct {
		name       string
		newContent []byte
	}{
		// Grown content: the stale size under-streams and the whole-file MD4 mismatches.
		{"grown", []byte("the CURRENT server content, modified again after the event fired - and longer")},
		// Shrunk content: the stale size over-streams and pre-fix the transfer stalled on a
		// zero-byte chunk before ever reaching verification.
		{"shrunk", []byte("short current content")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newTestStore(t)
			if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
				t.Fatalf("PutView: %v", err)
			}
			root := t.TempDir()
			c := &resumeDLClient{
				content:          map[string][]byte{"big": tc.newContent},
				mtime:            1700000456,
				chunkSize:        16,
				failOnceAtOffset: -1,
				queryNode: actions.Node{
					FileID: "big", Hash: md4.ContentHash(tc.newContent),
					Size: int64(len(tc.newContent)), MTime: 1700000456,
				},
			}
			e := New(st, &concTransport{client: c}, root, "sess", nil)
			e.limits.UploadResumeThreshold = 16

			// The op carries the EVENT-time snapshot: hash and size of the old content.
			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "big.bin",
				FileID: "big", LocalHash: md4.ContentHash(oldContent), LocalSize: int64(len(oldContent)),
				UnixPerm: 644, ServerSyncID: 5,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			if err := e.drainQueue(context.Background()); err != nil {
				t.Fatalf("drainQueue: %v (a stale event snapshot must not wedge the op)", err)
			}
			if c.queryCalls == 0 {
				t.Error("fresh QueryNode never issued before the chunked download")
			}
			got, err := os.ReadFile(filepath.Join(root, "Photos", "big.bin"))
			if err != nil || string(got) != string(tc.newContent) {
				t.Fatalf("big.bin = %q (err %v), want the current server content %q", got, err, tc.newContent)
			}
			b, err := st.GetBaseline(7, "big.bin")
			if err != nil {
				t.Fatalf("GetBaseline: %v", err)
			}
			if b.State != store.StateSynced {
				t.Errorf("state = %q, want synced", b.State)
			}
			// markSynced must record the facts of the bytes actually streamed, not the stale
			// event snapshot.
			if want := md4.ContentHash(tc.newContent); b.LocalHash != want || b.ServerHash != want {
				t.Errorf("baseline hashes = %q/%q, want %q", b.LocalHash, b.ServerHash, want)
			}
			if b.LocalSize != int64(len(tc.newContent)) {
				t.Errorf("baseline size = %d, want %d", b.LocalSize, len(tc.newContent))
			}
			if n, _ := st.PendingLen(); n != 0 {
				t.Errorf("queue not drained: %d ops remain", n)
			}
		})
	}
}

// TestInboundDownloadResumeEmptyHashWarnsAndRecordsRealFacts covers the empty-hash sibling: an
// event carrying no content hash (and no fresh facts available - the query fails cleanly) must
// not silently skip MD4 verification, and the synced baseline must record the promoted file's
// REAL hash - an empty LocalHash would make the very next scan re-detect a phantom change and
// re-upload the whole file.
func TestInboundDownloadResumeEmptyHashWarnsAndRecordsRealFacts(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	content := []byte("resume content whose event carried no hash, spanning several chunks")
	c := &resumeDLClient{
		content:          map[string][]byte{"big": content},
		mtime:            1700000789,
		chunkSize:        16,
		failOnceAtOffset: -1,
		queryErr:         errors.New("simulated clean query failure"),
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	e := New(st, &concTransport{client: c}, root, "sess", log)
	e.limits.UploadResumeThreshold = 16

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7, Path: "big.bin",
		FileID: "big", LocalHash: "", LocalSize: int64(len(content)),
		LocalMTime: 1700000789, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if got := logbuf.String(); !strings.Contains(got, "skipping MD4 verification") {
		t.Errorf("no skip-verification warning logged, got: %q", got)
	}
	b, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if want := md4.ContentHash(content); b.LocalHash != want || b.ServerHash != want {
		t.Errorf("baseline hashes = %q/%q, want the promoted file's real hash %q", b.LocalHash, b.ServerHash, want)
	}
	if b.LocalSize != int64(len(content)) {
		t.Errorf("baseline size = %d, want %d", b.LocalSize, len(content))
	}

	// The next scan must stay quiet: the baseline describes the on-disk file exactly, so no
	// spurious outbound upload is emitted.
	ops, err := e.localReconcile(context.Background(), 7)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("next scan emitted %d spurious ops: %+v", len(ops), ops)
	}
}
