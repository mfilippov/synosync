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

// TestResumeGetTokenViewScoped pins the request shape: a token is scoped to
// (session, view), so the request names the view - not a file, a size or a hash.
// The reply's handle may arrive under "resume_token" or the alternate "token" key.
func TestResumeGetTokenViewScoped(t *testing.T) {
	var got *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("resume_token", sproto.String("tok-1"))
	})

	tok, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 7, RootNodeID: 3})
	if err != nil {
		t.Fatalf("ResumeGetToken: %v", err)
	}
	if v, _ := got.Get("action"); v != sproto.String("resume_get_token") {
		t.Errorf("action = %v, want resume_get_token", v)
	}
	if v, _ := got.Get("view_id"); v != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", v)
	}
	if v, _ := got.Get("root_node_id"); v != sproto.Int(3) {
		t.Errorf("root_node_id = %v, want 3", v)
	}
	if v, _ := got.Get("root_node_path"); v != sproto.String("/") {
		t.Errorf("root_node_path = %v, want /", v)
	}
	// The token is not per-transfer, so no file/size/hash may be sent.
	for _, k := range []string{"path", "size", "hash"} {
		if _, ok := got.Get(k); ok {
			t.Errorf("request unexpectedly carried %q", k)
		}
	}
	if tok.Token != "tok-1" {
		t.Errorf("token = %+v, want tok-1", tok)
	}
}

func TestResumeGetTokenAlternateKey(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("token", sproto.String("tok-up"))
	})
	tok, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 1, RootNodeID: 1})
	if err != nil {
		t.Fatalf("ResumeGetToken: %v", err)
	}
	if tok.Token != "tok-up" {
		t.Errorf("token = %+v, want tok-up", tok)
	}
}

func TestResumeGetTokenServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeResumeToken))
	})
	_, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 1, RootNodeID: 1})
	if !errors.Is(err, ErrResumeToken) {
		t.Fatalf("err = %v, want ErrResumeToken", err)
	}
}

func TestResumeGetTokenMissingToken(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		// A success reply with no token handle is malformed.
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("offset", sproto.Int(0))
	})
	_, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 1, RootNodeID: 1})
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeGetTokenMalformedReply(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.String("not a map")
	})
	_, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 1, RootNodeID: 1})
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeGetTokenContextCanceled(t *testing.T) {
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
	if _, err := c.ResumeGetToken(ctx, ResumeRequest{ViewID: 1, RootNodeID: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("first turn err = %v, want context.Canceled", err)
	}
	// The interrupted turn poisons the connection: the next turn must refuse.
	if _, err := c.ResumeGetToken(testContext(t), ResumeRequest{ViewID: 1, RootNodeID: 1}); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("second turn err = %v, want ErrConnBroken", err)
	}
}

// The resume upload tests below exercise the WHOLE-FILE two-frame contract a live
// DSM implements: metadata frame (whole-file hash/size + resume_token) then a bare
// content frame streaming the entire file under file.data. There is no chunk loop
// and no offset - the earlier per-chunk/ResumeStatus model was never the protocol.

func testSpec(size int64, hash string) ResumeUploadSpec {
	return ResumeUploadSpec{ParentID: "p1", Name: "big.bin", MTime: 1700000000, RootNodeID: 1, Size: size, Hash: hash}
}

func TestResumeUploadWholeFile(t *testing.T) {
	content := []byte("the entire file, streamed in one content frame")
	hash := md4.ContentHash(content)
	var req, body *sproto.Map
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		req, body = r, b
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("newbig")).
			Set("file_size", sproto.Int(int64(len(content)))).
			Set("file_hash", sproto.String(hash))
	}))
	c := newClient(conn, "sess")

	node, err := c.ResumeUpload(testContext(t), "tok-up", testSpec(int64(len(content)), hash), bytes.NewReader(content))
	if err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}

	if v, _ := req.Get("action"); v != sproto.String("resume_upload") {
		t.Errorf("action = %v, want resume_upload", v)
	}
	if v, _ := req.Get("resume_token"); v != sproto.String("tok-up") {
		t.Errorf("resume_token = %v, want tok-up", v)
	}
	if v, _ := req.Get("path"); v != sproto.String("id:p1/big.bin") {
		t.Errorf("path = %v, want id:p1/big.bin", v)
	}
	// body-continue rides inside @proto, and the whole-file facts inside "file".
	rp, _ := req.Get("@proto")
	rpm, ok := rp.(*sproto.Map)
	if !ok {
		t.Fatalf("@proto = %T, want map", rp)
	}
	if v, _ := rpm.Get("body-continue"); v != sproto.Int(1) {
		t.Errorf("@proto.body-continue = %v, want 1", v)
	}
	rf, _ := req.Get("file")
	rfm, ok := rf.(*sproto.Map)
	if !ok {
		t.Fatalf("file = %T, want map", rf)
	}
	if v, _ := rfm.Get("size"); v != sproto.Int(int64(len(content))) {
		t.Errorf("file.size = %v, want %d", v, len(content))
	}
	if v, _ := rfm.Get("hash"); v != sproto.String(hash) {
		t.Errorf("file.hash = %v, want %s", v, hash)
	}
	// The metadata frame must NOT carry content.
	if _, ok := rfm.Get("data"); ok {
		t.Error("metadata frame carries file.data; the content belongs to frame 2 only")
	}

	// Frame 2: body envelope plus the whole content under file.data.
	bp, _ := body.Get("@proto")
	bpm, ok := bp.(*sproto.Map)
	if !ok {
		t.Fatalf("body @proto = %T, want map", bp)
	}
	if v, _ := bpm.Get("type"); v != sproto.String("body") {
		t.Errorf("body @proto.type = %v, want body", v)
	}
	bf, _ := body.Get("file")
	bfm, ok := bf.(*sproto.Map)
	if !ok {
		t.Fatalf("body file = %T, want map", bf)
	}
	data, _ := bfm.Get("data")
	bin, ok := data.(sproto.Binary)
	if !ok {
		t.Fatalf("body file.data = %T, want Binary", data)
	}
	if !bytes.Equal(bin.Data, content) {
		t.Errorf("body content = %q, want %q", bin.Data, content)
	}

	if node.FileID != "newbig" || node.Hash != hash {
		t.Errorf("node = %+v, want the created node's facts", node)
	}
}

func TestResumeUploadServerError(t *testing.T) {
	conn := serveMock(t, serveUpload(func(t *testing.T, _, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeConflictVersion))
	}))
	c := newClient(conn, "sess")
	_, err := c.ResumeUpload(testContext(t), "tok", testSpec(4, "h"), bytes.NewReader([]byte("abcd")))
	if !errors.Is(err, ErrConflictVersion) {
		t.Fatalf("err = %v, want ErrConflictVersion", err)
	}
	if !c.Broken() {
		t.Error("a rejected resume_upload must poison the connection (half-written body)")
	}
}

func TestResumeUploadNoVerdictReplyPoisons(t *testing.T) {
	// A reply carrying neither an error block nor type:"response" is no evidence the
	// transfer committed; it must be rejected and the connection poisoned.
	conn := serveMock(t, serveUpload(func(t *testing.T, _, _ *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("file_id", sproto.String("x"))
	}))
	c := newClient(conn, "sess")
	_, err := c.ResumeUpload(testContext(t), "tok", testSpec(4, "h"), bytes.NewReader([]byte("abcd")))
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
	if !c.Broken() {
		t.Error("a verdict-less reply must poison the connection")
	}
}

func TestResumeUploadUndrainedBodyDoesNotDeadlock(t *testing.T) {
	stall := make(chan struct{})
	// Closed by the deferred call below, which runs BEFORE serveMock's t.Cleanup
	// joins the server goroutine (cleanups are LIFO, defers run first).
	defer close(stall)
	// The server answers on the metadata frame alone and never drains the content,
	// so the concurrent body writer is wedged on backpressure. The turn must still
	// return (poison + close unblocks the writer) rather than hang forever.
	big := bytes.Repeat([]byte("x"), 1<<20)
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		dec := sproto.NewDecoder(server)
		if _, err := dec.Decode(); err != nil { // frame 1 only
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("file_id", sproto.String("x")))
		// Never drain the body, but do not park this goroutine forever: block on the
		// connection instead so serveMock's cleanup can unblock it by closing.
		<-stall
	})
	c := newClient(conn, "sess")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.ResumeUpload(testContext(t), "tok", testSpec(int64(len(big)), "h"), bytes.NewReader(big))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ResumeUpload deadlocked on an undrained body frame")
	}
}

func TestResumeUploadContextCanceled(t *testing.T) {
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		dec := sproto.NewDecoder(server)
		_, _ = dec.Decode()
		_, _ = dec.Decode()
		_, _ = io.Copy(io.Discard, server) // never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.ResumeUpload(ctx, "tok", testSpec(4, "h"), bytes.NewReader([]byte("abcd")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestResumeUploadStreamsWithoutBuffering(t *testing.T) {
	// The content must reach the wire straight from the reader: the encoder streams
	// Binary.Reader, so a body larger than any buffer still transfers exactly Size
	// bytes.
	const size = 3 << 20
	var got int
	conn := serveMock(t, serveUpload(func(t *testing.T, _, b *sproto.Map) sproto.Value {
		if bf, ok := b.Get("file"); ok {
			if bfm, ok := bf.(*sproto.Map); ok {
				if d, ok := bfm.Get("data"); ok {
					if bin, ok := d.(sproto.Binary); ok {
						// A payload past the decoder's inline cap is streamed past, so
						// Data is nil while Length still reports the true size.
						got = int(bin.Length)
					}
				}
			}
		}
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).Set("file_id", sproto.String("s"))
	}))
	c := newClient(conn, "sess")
	if _, err := c.ResumeUpload(testContext(t), "tok", testSpec(size, "h"), bytes.NewReader(bytes.Repeat([]byte("z"), size))); err != nil {
		t.Fatalf("ResumeUpload: %v", err)
	}
	if got != size {
		t.Errorf("server received %d content bytes, want %d", got, size)
	}
}

// resumeDownloadReply builds a resume_download reply in the shape a live DSM sends:
// the content and its facts inside the "file" sub-map.
func resumeDownloadReply(chunk []byte, hash string) *sproto.Map {
	return sproto.NewMap().
		Set("errno", sproto.Int(CodeOK)).
		Set("file_id", sproto.String("f1")).
		Set("name", sproto.String("big.bin")).
		Set("file", sproto.NewMap().
			Set("size", sproto.Int(int64(len(chunk)))).
			Set("hash", sproto.String(hash)).
			Set("data", sproto.Binary{Data: chunk}))
}

func TestResumeDownloadInline(t *testing.T) {
	chunk := []byte("a small resumed tail")
	var got *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return resumeDownloadReply(chunk, md4.ContentHash(chunk))
	})

	var buf bytes.Buffer
	node, err := c.ResumeDownload(testContext(t), "tok-d", "f1", 1, 0, 4096, &buf)
	if err != nil {
		t.Fatalf("ResumeDownload: %v", err)
	}
	if v, _ := got.Get("action"); v != sproto.String("resume_download") {
		t.Errorf("action = %v, want resume_download", v)
	}
	if v, _ := got.Get("path"); v != sproto.String("id:f1") {
		t.Errorf("path = %v, want id:f1", v)
	}
	if v, _ := got.Get("resume_token"); v != sproto.String("tok-d") {
		t.Errorf("resume_token = %v, want tok-d", v)
	}
	// The resume offset rides inside the "file" sub-map, like download's.
	rf, _ := got.Get("file")
	rfm, ok := rf.(*sproto.Map)
	if !ok {
		t.Fatalf("file = %T, want map", rf)
	}
	if v, _ := rfm.Get("offset"); v != sproto.Int(4096) {
		t.Errorf("file.offset = %v, want 4096", v)
	}
	if !bytes.Equal(buf.Bytes(), chunk) {
		t.Errorf("content = %q, want %q", buf.Bytes(), chunk)
	}
	if node.FileID != "f1" || node.Name != "big.bin" {
		t.Errorf("node = %+v, want file f1 big.bin", node)
	}
}

func TestResumeDownloadStreamed(t *testing.T) {
	// Larger than downloadInlineLimit so the codec takes the streaming path.
	chunk := bytes.Repeat([]byte("synosync"), downloadInlineLimit)
	if int64(len(chunk)) <= downloadInlineLimit {
		t.Fatalf("test chunk %d not larger than inline limit %d", len(chunk), downloadInlineLimit)
	}
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return resumeDownloadReply(chunk, md4.ContentHash(chunk))
	})

	var buf bytes.Buffer
	node, err := c.ResumeDownload(testContext(t), "tok-d", "big", 1, 0, 0, &buf)
	if err != nil {
		t.Fatalf("ResumeDownload: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), chunk) {
		t.Errorf("streamed content mismatch: got %d bytes, want %d", buf.Len(), len(chunk))
	}
	if node.Size != int64(len(chunk)) {
		t.Errorf("node.Size = %d, want %d", node.Size, len(chunk))
	}
}

func TestResumeDownloadHashMismatch(t *testing.T) {
	chunk := []byte("resumed bytes with a bad advertised hash")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return resumeDownloadReply(chunk, strings.Repeat("0", 32))
	})
	var buf bytes.Buffer
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("err = %v, want ErrHashMismatch", err)
	}
}

func TestResumeDownloadWriteError(t *testing.T) {
	chunk := []byte("resumed bytes")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return resumeDownloadReply(chunk, md4.ContentHash(chunk))
	})
	wantErr := errors.New("disk full")
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, errWriter{err: wantErr})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestResumeDownloadStreamedWriteErrorRealigns(t *testing.T) {
	// A destination write failure on the streamed path must not abandon the rest of
	// the reply on the wire: the decoder has to consume the whole payload so the
	// connection stays byte-aligned and a following turn reads cleanly. The first
	// resume_download write fails; a second turn on the same connection must succeed.
	big := bytes.Repeat([]byte("x"), downloadInlineLimit*2) // forces streaming
	small := []byte("second ok")
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		if err := sproto.NewEncoder(server).Encode(resumeDownloadReply(big, md4.ContentHash(big))); err != nil {
			return
		}
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(resumeDownloadReply(small, md4.ContentHash(small)))
	})
	c := newClient(conn, "sess")

	wantErr := errors.New("disk full")
	if _, err := c.ResumeDownload(testContext(t), "tok", "big", 1, 0, 0, errWriter{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("first download err = %v, want %v", err, wantErr)
	}

	var buf bytes.Buffer
	if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf); err != nil {
		t.Fatalf("second download err = %v, want nil (connection desynced?)", err)
	}
	if !bytes.Equal(buf.Bytes(), small) {
		t.Errorf("second content = %q, want %q", buf.Bytes(), small)
	}
}

func TestResumeDownloadNoServerHash(t *testing.T) {
	// When the server advertises no content hash, ResumeDownload skips verification
	// and still delivers the bytes. Locks the documented skip behaviour. The skip is
	// logged at Warn so the integrity gap is observable - assert that too (mirroring
	// Download).
	chunk := []byte("unverified resumed payload")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.Binary{Data: chunk})
	})
	var logbuf bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var buf bytes.Buffer
	node, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	if err != nil {
		t.Fatalf("ResumeDownload: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), chunk) {
		t.Errorf("content = %q, want %q", buf.Bytes(), chunk)
	}
	if node.Hash != "" {
		t.Errorf("node.Hash = %q, want empty", node.Hash)
	}
	if got := logbuf.String(); !strings.Contains(got, "skipping MD4 verification") {
		t.Errorf("expected a Warn about skipped verification, got %q", got)
	}
}

func TestResumeDownloadHashlessMultiChunkOneWarn(t *testing.T) {
	// A multi-chunk hash-less resume_download transfer must not flood the log: the
	// first chunk (offset 0) logs the skipped-verification note once at Warn, and
	// every later chunk demotes it to Debug, so a multi-GiB transfer emits exactly
	// one Warn, not one per chunk. The note itself still fires per
	// chunk (integrity gap stays fully observable at Debug); only its level changes.
	chunk := []byte("unverified resumed payload chunk")
	// A server that answers hash-less resume_download turns forever on one conn.
	c := newClient(serveMock(t, func(t *testing.T, server net.Conn) {
		for {
			if _, err := sproto.ReadHeader(server); err != nil {
				return
			}
			if _, err := sproto.NewDecoder(server).Decode(); err != nil {
				return
			}
			reply := sproto.NewMap().
				Set("errno", sproto.Int(CodeOK)).
				Set("file_id", sproto.String("f1")).
				Set("data", sproto.Binary{Data: chunk})
			if err := sproto.NewEncoder(server).Encode(reply); err != nil {
				return
			}
		}
	}), "sess")

	var logbuf bytes.Buffer
	// Debug-level handler so both the first-chunk Warn and the later-chunk Debug
	// records are captured and can be counted per level.
	c.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	offsets := []int64{0, int64(len(chunk)), int64(2 * len(chunk)), int64(3 * len(chunk))}
	for _, off := range offsets {
		var buf bytes.Buffer
		if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, off, &buf); err != nil {
			t.Fatalf("ResumeDownload(offset=%d): %v", off, err)
		}
		if !bytes.Equal(buf.Bytes(), chunk) {
			t.Fatalf("chunk at offset %d = %q, want %q", off, buf.Bytes(), chunk)
		}
	}

	out := logbuf.String()
	if warns := strings.Count(out, "level=WARN"); warns != 1 {
		t.Errorf("WARN lines = %d, want 1 (only the first chunk warns); log:\n%s", warns, out)
	}
	if debugs := strings.Count(out, "level=DEBUG"); debugs != len(offsets)-1 {
		t.Errorf("DEBUG lines = %d, want %d (later chunks demote to Debug); log:\n%s", debugs, len(offsets)-1, out)
	}
	if skips := strings.Count(out, "skipping MD4 verification"); skips != len(offsets) {
		t.Errorf("skip notes = %d, want %d (one per chunk, gap stays observable); log:\n%s", skips, len(offsets), out)
	}
}

func TestResumeDownloadDataNotBinary(t *testing.T) {
	// A success reply whose "data" field is not a Binary is malformed.
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.String("not binary"))
	})
	var buf bytes.Buffer
	if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf); !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeDownloadForeignBinaryWithData(t *testing.T) {
	// "data" is present and small (inline) but the reply also smuggles an oversized
	// binary that streams through the key-blind callback. The streamed binary does
	// not match the "data" field length, so the count/length guard must reject it
	// rather than serve the foreign bytes as content.
	data := []byte("real chunk")
	foreign := bytes.Repeat([]byte("y"), downloadInlineLimit*2)
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("extra", sproto.Binary{Data: foreign}).
			Set("data", sproto.Binary{Data: data})
	})
	var buf bytes.Buffer
	if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf); !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeDownloadServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeResumeToken))
	})
	var buf bytes.Buffer
	_, err := c.ResumeDownload(testContext(t), "stale", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, ErrResumeToken) {
		t.Fatalf("err = %v, want ErrResumeToken", err)
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %d bytes on error reply, want 0", buf.Len())
	}
}

func TestResumeDownloadMissingData(t *testing.T) {
	// A success reply with no data field is malformed: it must surface as
	// ErrUnexpectedReply rather than a silently-empty chunk.
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1"))
	})
	var buf bytes.Buffer
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeDownloadMalformedReply(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.String("not a map")
	})
	var buf bytes.Buffer
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestResumeDownloadContextCanceled(t *testing.T) {
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
	_, err := c.ResumeDownload(ctx, "tok", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// The interrupted turn poisons the connection.
	if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("second turn err = %v, want ErrConnBroken", err)
	}
}

// TestResumeDownloadChunkedWithWholeFileHashSucceeds pins that a genuinely chunked transfer
// completes.
//
// The reply advertises the NODE's hash, which is the only hash the protocol offers, so a chunk
// can never match it - the chunk is a slice of the file. Comparing them made every chunked
// transfer fail with ErrHashMismatch on its very first chunk, so the verb only worked against a
// server that returns the entire remainder in one reply. Nothing measured says a live DSM does
// that, and the only multi-chunk test in this package used a hash-LESS fake, which is exactly
// the case that skipped the comparison and hid the defect.
//
// Verification of a chunked transfer belongs to whoever reassembles it; the engine hashes the
// whole file before promoting the staged copy.
func TestResumeDownloadChunkedWithWholeFileHashSucceeds(t *testing.T) {
	whole := []byte("the complete file content, delivered in two halves")
	half := len(whole) / 2
	wholeHash := md4.ContentHash(whole)

	var calls int
	c := newClient(serveMock(t, func(t *testing.T, server net.Conn) {
		for {
			if _, err := sproto.ReadHeader(server); err != nil {
				return
			}
			if _, err := sproto.NewDecoder(server).Decode(); err != nil {
				return
			}
			// Both chunks advertise the WHOLE file's hash and size, which is what a
			// node-describing reply carries.
			body := whole[:half]
			if calls > 0 {
				body = whole[half:]
			}
			calls++
			reply := sproto.NewMap().
				Set("errno", sproto.Int(CodeOK)).
				Set("file_id", sproto.String("f1")).
				Set("size", sproto.Int(int64(len(whole)))).
				Set("hash", sproto.String(wholeHash)).
				Set("data", sproto.Binary{Data: body})
			if err := sproto.NewEncoder(server).Encode(reply); err != nil {
				return
			}
		}
	}), "sess")

	var got bytes.Buffer
	for _, off := range []int64{0, int64(half)} {
		if _, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, off, &got); err != nil {
			t.Fatalf("ResumeDownload(offset=%d): %v", off, err)
		}
	}
	if !bytes.Equal(got.Bytes(), whole) {
		t.Fatalf("reassembled = %q, want %q", got.Bytes(), whole)
	}
}

// TestResumeDownloadSingleReplyStillVerifies is the other half: when the reply IS the whole
// node, the comparison is meaningful and still catches corrupted bytes at the turn.
func TestResumeDownloadSingleReplyStillVerifies(t *testing.T) {
	content := []byte("the whole file in one reply")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("size", sproto.Int(int64(len(content)))).
			Set("hash", sproto.String("00000000000000000000000000000000")).
			Set("data", sproto.Binary{Data: content})
	})

	var buf bytes.Buffer
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	if !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("err = %v, want ErrHashMismatch on a whole-node reply", err)
	}
}

// TestResumeUploadBodyFailureSurfacesAtOnce pins that a local read failure ends the turn
// immediately instead of waiting out the idle budget.
//
// The body is written by a second goroutine while the first blocks on the reply. When the body
// fails - the file shrank, the read errored - the server is left waiting for bytes that will
// never arrive and never replies, so neither side moves and the only thing that ended the turn
// was actionIdleTimeout: five minutes per attempt, on a failure known immediately, with a
// worker lease held throughout and the op re-wedging it on the next drain.
func TestResumeUploadBodyFailureSurfacesAtOnce(t *testing.T) {
	// A server that accepts the metadata frame and then says nothing at all, which is what one
	// waiting for an unfinished body does.
	c := newClient(serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		// Never reply; never drain. Block until the client tears the connection down.
		_, _ = io.Copy(io.Discard, server)
	}), "sess")

	boom := errors.New("the local file shrank")
	body := io.MultiReader(strings.NewReader("some"), failingReader{err: boom})

	done := make(chan error, 1)
	go func() {
		_, err := c.ResumeUpload(testContext(t), "tok", ResumeUploadSpec{
			ParentID: "p", Name: "big.bin", Size: 1 << 20, Hash: "h",
		}, body)
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ResumeUpload succeeded despite a failed body read")
		}
		if !errors.Is(err, boom) {
			t.Errorf("err = %v, want it to carry the body's own failure", err)
		}
	case <-time.After(20 * time.Second):
		// Far below actionIdleTimeout: the point is that it does NOT wait for the budget.
		t.Fatal("ResumeUpload did not return promptly; it is waiting out the idle budget")
	}
}

// failingReader yields err after the reader before it is exhausted.
type failingReader struct{ err error }

func (f failingReader) Read([]byte) (int, error) { return 0, f.err }
