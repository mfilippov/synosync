package actions

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/sproto"
)

// errWriter is an io.Writer that always fails, used to drive the download
// write-error path.
type errWriter struct{ err error }

func (w errWriter) Write([]byte) (int, error) { return 0, w.err }

// downloadReply builds a success download reply carrying content and metadata.
// hash is the content hash the server advertises; tests pass a correct or a
// deliberately wrong value.
func downloadReply(content []byte, hash string) *sproto.Map {
	return sproto.NewMap().
		Set("errno", sproto.Int(CodeOK)).
		Set("file_id", sproto.String("f1")).
		Set("name", sproto.String("a.txt")).
		Set("size", sproto.Int(int64(len(content)))).
		Set("hash", sproto.String(hash)).
		Set("mtime", sproto.Int(1700000000)).
		Set("data", sproto.Binary{Data: content})
}

func TestDownloadSmall(t *testing.T) {
	content := []byte("hello world")
	var gotPath, gotAction sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotAction, _ = req.Get("action")
		gotPath, _ = req.Get("path")
		return downloadReply(content, md4.ContentHash(content))
	})

	var buf bytes.Buffer
	node, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if gotAction != sproto.String("download") {
		t.Errorf("action = %v, want download", gotAction)
	}
	if gotPath != sproto.String("id:f1") {
		t.Errorf("path = %v, want id:f1", gotPath)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("content = %q, want %q", buf.Bytes(), content)
	}
	if node.FileID != "f1" || node.Name != "a.txt" || node.Size != int64(len(content)) {
		t.Errorf("node = %+v, want file f1 a.txt size %d", node, len(content))
	}
}

func TestDownloadStreamed(t *testing.T) {
	// Larger than downloadInlineLimit so the codec takes the streaming path.
	content := bytes.Repeat([]byte("synosync"), downloadInlineLimit) // 8 * 4096 bytes
	if int64(len(content)) <= downloadInlineLimit {
		t.Fatalf("test content %d not larger than inline limit %d", len(content), downloadInlineLimit)
	}
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return downloadReply(content, md4.ContentHash(content))
	})

	var buf bytes.Buffer
	node, err := c.Download(testContext(t), "big", 1, 0, &buf)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("streamed content mismatch: got %d bytes, want %d", buf.Len(), len(content))
	}
	if node.Size != int64(len(content)) {
		t.Errorf("node.Size = %d, want %d", node.Size, len(content))
	}
}

func TestDownloadHashMismatch(t *testing.T) {
	content := []byte("hello world")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return downloadReply(content, "00000000000000000000000000000000")
	})

	var buf bytes.Buffer
	_, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("err = %v, want ErrHashMismatch", err)
	}
}

func TestDownloadServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})

	var buf bytes.Buffer
	_, err := c.Download(testContext(t), "missing", 1, 0, &buf)
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes on error reply, want 0", buf.Len())
	}
}

func TestDownloadNoServerHash(t *testing.T) {
	// When the server advertises no content hash, Download skips verification and
	// still delivers the bytes. This locks the documented behaviour so a regression
	// that flipped the guard (always-skip or always-fail) would be caught. The skip
	// is now logged at Warn so the integrity gap is observable - assert that too.
	content := []byte("unverified payload")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.Binary{Data: content})
	})
	var logbuf bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var buf bytes.Buffer
	node, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("content = %q, want %q", buf.Bytes(), content)
	}
	if node.Hash != "" {
		t.Errorf("node.Hash = %q, want empty", node.Hash)
	}
	if got := logbuf.String(); !strings.Contains(got, "skipping MD4 verification") {
		t.Errorf("expected a Warn about skipped verification, got %q", got)
	}
}

func TestDownloadVerifiesViaSendHash(t *testing.T) {
	// The server may carry the content hash only in the content Binary's BinaryEx
	// (0x43) send_hash frame, with no map "hash" field. Download must fall back to
	// that hash and verify, rather than log "skipping MD4 verification" for a hash
	// the server actually advertised.
	content := []byte("hash lives in send_hash")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.Binary{Data: content, SendHash: md4.ContentHash(content)})
	})
	var logbuf bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); err != nil {
		t.Fatalf("Download verified via send_hash: %v, want nil", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("content = %q, want %q", buf.Bytes(), content)
	}
	if strings.Contains(logbuf.String(), "skipping MD4 verification") {
		t.Errorf("verification was skipped despite a send_hash; log: %q", logbuf.String())
	}
}

func TestDownloadSendHashMismatch(t *testing.T) {
	// A wrong BinaryEx send_hash (and no map hash) must still be caught: the fallback
	// verifies rather than trusts the bytes.
	content := []byte("payload")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.Binary{Data: content, SendHash: strings.Repeat("0", 32)})
	})
	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("err = %v, want ErrHashMismatch", err)
	}
}

func TestDownloadMapHashPreferredOverSendHash(t *testing.T) {
	// When both the map "hash" field and a BinaryEx send_hash are present, the map
	// field wins (send_hash is only a fallback). A correct map hash with a bogus
	// send_hash must still verify.
	content := []byte("both present")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("hash", sproto.String(md4.ContentHash(content))).
			Set("data", sproto.Binary{Data: content, SendHash: strings.Repeat("0", 32)})
	})
	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); err != nil {
		t.Fatalf("Download with correct map hash + bogus send_hash: %v, want nil", err)
	}
}

func TestDownloadStreamedVerifiesViaSendHash(t *testing.T) {
	// The send_hash fallback must also fire on the streamed (large) path: the inner
	// binary streams through the callback while the flattened BinaryEx carries the
	// hash, so verification runs against the streamed bytes.
	content := bytes.Repeat([]byte("z"), downloadInlineLimit*2) // forces streaming
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("big")).
			Set("data", sproto.Binary{Data: content, SendHash: md4.ContentHash(content)})
	})
	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "big", 1, 0, &buf); err != nil {
		t.Fatalf("streamed Download verified via send_hash: %v, want nil", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("streamed content mismatch: got %d bytes, want %d", buf.Len(), len(content))
	}
}

func TestDownloadWriteError(t *testing.T) {
	// A failing destination writer surfaces as an error from the inline write path.
	content := []byte("hello world")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return downloadReply(content, md4.ContentHash(content))
	})

	wantErr := errors.New("disk full")
	_, err := c.Download(testContext(t), "f1", 1, 0, errWriter{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestDownloadStreamedWriteErrorRealigns(t *testing.T) {
	// A destination write failure on the streamed path must not abandon the rest
	// of the reply on the wire: the decoder has to consume the whole payload so the
	// connection stays byte-aligned and a following action reads cleanly. The server
	// answers two download turns on the same connection; the first client write
	// fails, the second must still succeed.
	big := bytes.Repeat([]byte("x"), downloadInlineLimit*2) // forces streaming
	small := []byte("second ok")
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		// Turn 1: large reply (streamed on the client).
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		if err := sproto.NewEncoder(server).Encode(downloadReply(big, md4.ContentHash(big))); err != nil {
			return
		}
		// Turn 2: small reply on the same connection.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(downloadReply(small, md4.ContentHash(small)))
	})
	c := newClient(conn, "sess")

	wantErr := errors.New("disk full")
	if _, err := c.Download(testContext(t), "big", 1, 0, errWriter{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("first download err = %v, want %v", err, wantErr)
	}

	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); err != nil {
		t.Fatalf("second download err = %v, want nil (connection desynced?)", err)
	}
	if !bytes.Equal(buf.Bytes(), small) {
		t.Errorf("second content = %q, want %q", buf.Bytes(), small)
	}
}

func TestDownloadMissingData(t *testing.T) {
	// A success reply with no data field is malformed: it must surface as
	// ErrUnexpectedReply rather than a silently-empty download.
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1"))
	})
	var buf bytes.Buffer
	_, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestDownloadForeignBinaryNoData(t *testing.T) {
	// A success reply with no "data" field but another oversized binary must not be
	// reported as a successful (silently wrong) download. The foreign binary streams
	// through the BinaryStream callback (which is key-blind), but validation requires
	// the "data" field unconditionally - so this is ErrUnexpectedReply, not success.
	foreign := bytes.Repeat([]byte("y"), downloadInlineLimit*2) // forces streaming
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("extra", sproto.Binary{Data: foreign})
	})
	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestDownloadForeignBinaryWithData(t *testing.T) {
	// "data" is present and small (inline) but the reply also smuggles an oversized
	// binary that streams through the callback. The streamed binary does not match
	// the "data" field (different length), so the length/count correlation must
	// reject the reply rather than serve the foreign bytes as content.
	data := []byte("real content")
	foreign := bytes.Repeat([]byte("y"), downloadInlineLimit*2)
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("extra", sproto.Binary{Data: foreign}).
			Set("data", sproto.Binary{Data: data})
	})
	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestDownloadContextCanceled(t *testing.T) {
	// Cancelling ctx mid-turn must interrupt the blocking reply read and surface as
	// context.Canceled - the download path wires watchContext/contextErr itself,
	// independently of do.
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	var buf bytes.Buffer
	_, err := c.Download(ctx, "f1", 1, 0, &buf)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestDownloadContextDeadline(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // read the request but never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var buf bytes.Buffer
	_, err := c.Download(ctx, "f1", 1, 0, &buf)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestDownloadStreamedHashMismatch(t *testing.T) {
	// A streamed (large) payload must be hashed during streaming too, so a wrong
	// advertised hash is still caught on the streaming path.
	content := bytes.Repeat([]byte("x"), downloadInlineLimit*2)
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return downloadReply(content, strings.Repeat("0", 32))
	})
	var buf bytes.Buffer
	_, err := c.Download(testContext(t), "big", 1, 0, &buf)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("err = %v, want ErrHashMismatch", err)
	}
}

// TestDownloadUppercaseServerHashVerifies is the reported case: a DSM
// build advertising the content hash in uppercase hex must not fail every
// download with ErrHashMismatch. decodeNode canonicalizes the hash to
// lowercase, so the verify against the locally computed lowercase MD4 passes
// and the returned node carries the canonical form.
func TestDownloadUppercaseServerHashVerifies(t *testing.T) {
	content := []byte("hello world")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return downloadReply(content, strings.ToUpper(md4.ContentHash(content)))
	})

	var buf bytes.Buffer
	node, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	if err != nil {
		t.Fatalf("Download with uppercase server hash: %v, want nil", err)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("content = %q, want %q", buf.Bytes(), content)
	}
	if node.Hash != md4.ContentHash(content) {
		t.Errorf("node.Hash = %q, want canonical lowercase %q", node.Hash, md4.ContentHash(content))
	}
}

// TestDownloadWireShape pins the scoping fields a live DSM requires on a `download`
// request. It is a LOAD-BEARING test, not a formality, and it is the safety net the
// TRANSIENT reading of 32772 names for itself (errors.go, CodeNodeBusy).
//
// 32772 carries two meanings on one number: the measured transient ("the node's
// content is still settling, ask again") and a malformed request - a download whose
// params omit the (root_node_id, root_node_path) scoping draws it too. The engine now
// resolves that ambiguity permanently in favour of the transient:
// engine.serverTransientRejection denies 32772 the deterministic-rejection escalation,
// so an op drawing it can never dead-letter. A regression that dropped a scoping field
// would therefore not surface as a dropped op - it would retry forever, paced but
// wedged, holding every path-overlapping successor behind it. Nothing in the running
// system can tell the two meanings apart, so the request shape is pinned here instead.
func TestDownloadWireShape(t *testing.T) {
	content := []byte("hello")
	var got *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return downloadReply(content, md4.ContentHash(content))
	})
	if _, err := c.Download(testContext(t), "f1", 9, 42, io.Discard); err != nil {
		t.Fatalf("Download: %v", err)
	}
	// The scoping block: without it the server answers 32772, which the engine now
	// reads as "busy, retry" forever.
	if v, _ := got.Get("root_node_id"); v != sproto.Int(9) {
		t.Errorf("root_node_id = %v, want the caller's view root 9", v)
	}
	if v, _ := got.Get("root_node_path"); v != sproto.String("/") {
		t.Errorf("root_node_path = %v, want \"/\"", v)
	}
	if v, _ := got.Get("path"); v != sproto.String("id:f1") {
		t.Errorf("path = %v, want the id-addressed form id:f1", v)
	}
	// max_id=0 disables the version upper bound the server would otherwise enforce;
	// target_sync_id carries the caller's position.
	if v, _ := got.Get("max_id"); v != sproto.Int(0) {
		t.Errorf("max_id = %v, want 0 (no version upper bound)", v)
	}
	if v, _ := got.Get("target_sync_id"); v != sproto.Int(42) {
		t.Errorf("target_sync_id = %v, want 42", v)
	}
	// The offset lives INSIDE the file sub-map, never as a top-level key.
	if _, ok := got.Get("offset"); ok {
		t.Error("request carried a top-level offset; the server reads file.offset")
	}
	file, ok := got.Get("file")
	if !ok {
		t.Fatal("request carried no file sub-map")
	}
	fm, ok := file.(*sproto.Map)
	if !ok {
		t.Fatalf("file = %T, want *sproto.Map", file)
	}
	if v, _ := fm.Get("offset"); v != sproto.Int(0) {
		t.Errorf("file.offset = %v, want 0 for a whole-file download", v)
	}
}

// TestResumeDownloadWireShape pins the same scoping block on resume_download, plus the
// two fields that distinguish it from a plain download: the resume token and the offset
// inside the file sub-map. Same stakes as TestDownloadWireShape - a lost scoping field
// draws 32772 and the engine can no longer read that as an error.
func TestResumeDownloadWireShape(t *testing.T) {
	content := []byte("tail bytes")
	var got *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return downloadReply(content, md4.ContentHash(content))
	})
	if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 9, 42, 1024, io.Discard); err != nil {
		t.Fatalf("ResumeDownload: %v", err)
	}
	if v, _ := got.Get("action"); v != sproto.String("resume_download") {
		t.Errorf("action = %v, want resume_download", v)
	}
	if v, _ := got.Get("resume_token"); v != sproto.String("tok") {
		t.Errorf("resume_token = %v, want tok", v)
	}
	if v, _ := got.Get("root_node_id"); v != sproto.Int(9) {
		t.Errorf("root_node_id = %v, want 9", v)
	}
	if v, _ := got.Get("root_node_path"); v != sproto.String("/") {
		t.Errorf("root_node_path = %v, want \"/\"", v)
	}
	if _, ok := got.Get("offset"); ok {
		t.Error("request carried a top-level offset; the server reads file.offset")
	}
	file, ok := got.Get("file")
	if !ok {
		t.Fatal("request carried no file sub-map")
	}
	fm, ok := file.(*sproto.Map)
	if !ok {
		t.Fatalf("file = %T, want *sproto.Map", file)
	}
	if v, _ := fm.Get("offset"); v != sproto.Int(1024) {
		t.Errorf("file.offset = %v, want the resume offset 1024", v)
	}
}
