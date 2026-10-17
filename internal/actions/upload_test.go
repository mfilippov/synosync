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

// uploadHandler receives the decoded frame-1 metadata request and frame-2 body
// frame and returns the reply Value to encode as a bare PObject. Returning nil
// writes no reply.
type uploadHandler func(t *testing.T, req, body *sproto.Map) sproto.Value

// serveUpload adapts an uploadHandler into a serve function: it reads the
// CmdAction header, decodes the metadata frame and the body frame from the same
// stream, hands both to handle, and writes handle's return value as the reply.
func serveUpload(handle uploadHandler) func(t *testing.T, server net.Conn) {
	return func(t *testing.T, server net.Conn) {
		h, err := sproto.ReadHeader(server)
		if err != nil {
			return
		}
		if h.Cmd != sproto.CmdAction {
			t.Errorf("request cmd = 0x%02x, want CmdAction (0x%02x)", h.Cmd, sproto.CmdAction)
		}
		dec := sproto.NewDecoder(server)
		v1, err := dec.Decode()
		if err != nil {
			return
		}
		req, _ := v1.(*sproto.Map)
		v2, err := dec.Decode()
		if err != nil {
			return
		}
		body, _ := v2.(*sproto.Map)
		reply := handle(t, req, body)
		if reply == nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(reply)
	}
}

func TestUploadTwoFrame(t *testing.T) {
	content := []byte("hello upload content")
	var req, body *sproto.Map
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		req, body = r, b
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("newf")).
			Set("name", sproto.String("hello.txt")).
			Set("size", sproto.Int(int64(len(content)))).
			Set("hash", sproto.String(md4.ContentHash(content))).
			Set("mtime", sproto.Int(1700000000))
	}))
	c := newClient(conn, "sess")

	node, err := c.Upload(testContext(t), "parent1", "hello.txt", 0, 1, content)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Frame 1: metadata request fields.
	if v, _ := req.Get("action"); v != sproto.String("upload") {
		t.Errorf("action = %v, want upload", v)
	}
	if v, _ := req.Get("session"); v != sproto.String("sess") {
		t.Errorf("session = %v, want sess", v)
	}
	if v, _ := req.Get("path"); v != sproto.String("id:parent1/hello.txt") {
		t.Errorf("path = %v, want id:parent1/hello.txt", v)
	}
	// body-continue lives inside the @proto envelope, and the content facts inside
	// the "file" sub-map - the shape a live DSM parses.
	reqProto, ok := req.Get("@proto")
	if !ok {
		t.Fatal("metadata frame missing @proto envelope")
	}
	rpm, ok := reqProto.(*sproto.Map)
	if !ok {
		t.Fatalf("@proto = %T, want map", reqProto)
	}
	if v, _ := rpm.Get("body-continue"); v != sproto.Int(1) {
		t.Errorf("@proto.body-continue = %v, want 1", v)
	}
	rawFile, ok := req.Get("file")
	if !ok {
		t.Fatal("metadata frame missing file sub-map")
	}
	fm, ok := rawFile.(*sproto.Map)
	if !ok {
		t.Fatalf("file = %T, want map", rawFile)
	}
	if v, _ := fm.Get("size"); v != sproto.Int(int64(len(content))) {
		t.Errorf("file.size = %v, want %d", v, len(content))
	}
	if v, _ := fm.Get("hash"); v != sproto.String(md4.ContentHash(content)) {
		t.Errorf("file.hash = %v, want %s", v, md4.ContentHash(content))
	}
	if v, _ := req.Get("file_type"); v != sproto.String("file") {
		t.Errorf("file_type = %v, want file", v)
	}

	// Frame 2: body frame carries the @proto envelope and the inline bytes.
	proto, ok := body.Get("@proto")
	if !ok {
		t.Fatalf("body frame missing @proto envelope")
	}
	pm, ok := proto.(*sproto.Map)
	if !ok {
		t.Fatalf("@proto = %T, want map", proto)
	}
	if v, _ := pm.Get("type"); v != sproto.String("body") {
		t.Errorf("@proto.type = %v, want body", v)
	}
	// The bytes ride under file.data on the continuation frame, which re-sends the
	// whole metadata block.
	rawBodyFile, ok := body.Get("file")
	if !ok {
		t.Fatal("body frame missing file sub-map")
	}
	bfm, ok := rawBodyFile.(*sproto.Map)
	if !ok {
		t.Fatalf("body file = %T, want map", rawBodyFile)
	}
	data, _ := bfm.Get("data")
	bin, ok := data.(sproto.Binary)
	if !ok {
		t.Fatalf("body file.data = %T, want Binary", data)
	}
	if !bytes.Equal(bin.Data, content) {
		t.Errorf("body content = %q, want %q", bin.Data, content)
	}

	// Reply node metadata.
	if node.FileID != "newf" || node.Name != "hello.txt" || node.Size != int64(len(content)) {
		t.Errorf("node = %+v, want file newf hello.txt size %d", node, len(content))
	}
}

func TestUploadConflict(t *testing.T) {
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeConflictVersion))
	}))
	c := newClient(conn, "sess")
	_, err := c.Upload(testContext(t), "p", "n", 0, 1, []byte("x"))
	if !errors.Is(err, ErrConflictVersion) {
		t.Fatalf("err = %v, want ErrConflictVersion", err)
	}
}

func TestUploadServerError(t *testing.T) {
	// The server reads both frames, then rejects the operation with an error
	// code; the client must surface it as the matching typed error.
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	}))
	c := newClient(conn, "sess")
	_, err := c.Upload(testContext(t), "p", "n", 0, 1, []byte("data"))
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

func TestUploadRejectsOversizedInlineBinary(t *testing.T) {
	// An upload reply carries only metadata; a Binary past maxReplyInlineBinary is
	// hostile/corrupt. The client must drain it (not buffer the whole value), reject
	// the reply as ErrUnexpectedReply, and mark the connection broken - the concurrent
	// body writer can't be trusted after such a reply.
	const big = maxReplyInlineBinary + 1
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("data", sproto.Binary{Data: make([]byte, big)})
	}))
	c := newClient(conn, "sess")

	_, err := c.Upload(testContext(t), "p", "n", 0, 1, []byte("data"))
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("Upload err = %v, want ErrUnexpectedReply", err)
	}
	if !c.Broken() {
		t.Fatal("connection not marked broken after an oversized upload reply")
	}
}

func TestUploadEarlyRejection(t *testing.T) {
	// The server rejects on the metadata frame alone and replies WITHOUT draining
	// the body frame. The body is larger than any socket buffer, so a sequential
	// write-body-then-read would block on backpressure with the typed error sitting
	// unread; the concurrent write must let the reply through. The client must
	// surface the typed error and mark the connection broken.
	big := bytes.Repeat([]byte("x"), 1<<16)
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil { // frame 1 only
			return
		}
		// Reply immediately; never read the body frame.
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("errno", sproto.Int(CodePathError)))
		_, _ = io.Copy(io.Discard, server)
	})
	c := newClient(conn, "sess")

	_, err := c.Upload(testContext(t), "p", "n", 0, 1, big)
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
	// The half-written body desyncs the connection: a later turn must refuse.
	if _, err := c.Mkdir(testContext(t), "p", "n2", 0, 1); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("after early rejection, Mkdir err = %v, want ErrConnBroken", err)
	}
}

func TestUploadRejectsErrnoLessReply(t *testing.T) {
	// A flaky/non-conforming DSM that replied without an errno (here even echoing
	// node fields) must NOT be accepted as a committed upload: like the mutating
	// rename/remove/unlink turns, a present errno is the only positive evidence the
	// server performed the write. The client must reject the reply as
	// ErrUnexpectedReply. Unlike the single-frame doMutate verbs, Upload wrote a body
	// frame the server may not have drained, so the connection is poisoned:
	// its own framing state is undecidable from here, so it must not be reused.
	content := []byte("data")
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("file_id", sproto.String("newf")).
			Set("name", sproto.String("n")).
			Set("size", sproto.Int(int64(len(content)))).
			Set("hash", sproto.String(md4.ContentHash(content)))
	}))
	c := newClient(conn, "sess")

	_, err := c.Upload(testContext(t), "p", "n", 0, 1, content)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("Upload err = %v, want ErrUnexpectedReply", err)
	}
	if !c.Broken() {
		t.Fatal("connection not marked broken after an errno-less two-frame reply; the possibly-undrained body leaves the server one reply out of step, so it must poison")
	}
	// A later turn must refuse rather than reuse the possibly server-desynced socket.
	if _, err := c.Mkdir(testContext(t), "p", "n2", 0, 1); !errors.Is(err, ErrConnBroken) {
		t.Fatalf("after errno-less reject, Mkdir err = %v, want ErrConnBroken", err)
	}
}

func TestUploadErrnoLessReplyUndrainedBodyDoesNotDeadlock(t *testing.T) {
	// A non-conforming DSM replies with an errno-less Map on the metadata
	// frame ALONE and never drains the body frame. With a body larger than the socket
	// send buffer (net.Pipe blocks every unread write), the concurrent body writer
	// wedges on backpressure. The errno-presence check must run BEFORE joining the
	// writer and close the connection first - otherwise the join blocks forever when
	// no turn deadline forces the socket (limits.op_turn_timeout_s=0, modelled here by
	// context.Background(), whose Done() is nil so netctx.Watch installs no deadline).
	// The fixed client returns ErrUnexpectedReply promptly and marks the conn broken.
	//
	// The server stays alive on <-release rather than returning: a returning server
	// would close its end and unblock the writer, hiding the deadlock.
	big := bytes.Repeat([]byte("x"), 1<<16)
	release := make(chan struct{})
	defer close(release)
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil { // frame 1 only
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().Set("file_id", sproto.String("newf")))
		<-release
	})
	c := newClient(conn, "sess")

	done := make(chan error, 1)
	go func() {
		_, err := c.Upload(context.Background(), "p", "n", 0, 1, big)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrUnexpectedReply) {
			t.Fatalf("Upload err = %v, want ErrUnexpectedReply", err)
		}
		if !c.Broken() {
			t.Fatal("connection not marked broken after an errno-less reply with an undrained body")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Upload did not return: an errno-less reply with an undrained body deadlocked the worker (join-before-check)")
	}
}

func TestUploadSuccessUndrainedBodyDoesNotDeadlock(t *testing.T) {
	// The success branch the errno-less test does not cover. The server answers errno:0 - a
	// well-formed success carrying its required evidence - on the metadata frame ALONE
	// and never drains the body frame. The body is larger than any socket buffer
	// (net.Pipe blocks on every unread write), so the concurrent writer is wedged on
	// backpressure. The success path joined that writer unguarded, which hangs the
	// worker forever when no turn deadline forces the socket (limits.op_turn_timeout_s
	// = 0, the default, modelled here by context.Background(): its Done() is nil, so
	// netctx.Watch installs no deadline).
	//
	// The bounded join must instead expire, close the connection to unblock the writer,
	// and reject the turn as ErrUnexpectedReply with the connection poisoned - a claim
	// of a committed write is not believable when our body never reached the server.
	//
	// The server stays alive on <-release rather than returning: a returning server
	// would close its end and unblock the writer, hiding the deadlock.
	shortBodyJoinGrace(t, 50*time.Millisecond)
	big := bytes.Repeat([]byte("x"), 1<<16)
	release := make(chan struct{})
	defer close(release)
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil { // frame 1 only
			return
		}
		_ = sproto.NewEncoder(server).Encode(sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("newf")))
		<-release
	})
	c := newClient(conn, "sess")

	done := make(chan error, 1)
	go func() {
		_, err := c.Upload(context.Background(), "p", "n", 0, 1, big)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrUnexpectedReply) {
			t.Fatalf("Upload err = %v, want ErrUnexpectedReply", err)
		}
		if !c.Broken() {
			t.Fatal("connection not marked broken after a success reply with an undrained body")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Upload did not return: an errno:0 reply with an undrained body deadlocked the worker (unbounded success-path join)")
	}
}

func TestUploadSuccessKeepsConnectionReusable(t *testing.T) {
	// The success-join bound must not disturb the normal path: a server that drains the body
	// and confirms with errno:0 completes the join immediately, leaves the Client
	// un-poisoned, and the same connection serves the next turn. The body is deliberately
	// larger than a pipe write so the writer really does have to be joined, not merely
	// found already-finished by luck of scheduling.
	shortBodyJoinGrace(t, 50*time.Millisecond)
	content := bytes.Repeat([]byte("y"), 1<<16)
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		// Turn 1: the upload, both frames read.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		dec := sproto.NewDecoder(server)
		if _, err := dec.Decode(); err != nil { // frame 1: metadata
			return
		}
		if _, err := dec.Decode(); err != nil { // frame 2: body
			return
		}
		enc := sproto.NewEncoder(server)
		if err := enc.Encode(sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("newf"))); err != nil {
			return
		}
		// Turn 2: an ordinary single-frame mkdir over the SAME connection.
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		// Mkdir is a two-frame turn as well (metadata + an empty body continuation),
		// so drain both before replying.
		if _, err := dec.Decode(); err != nil {
			return
		}
		if _, err := dec.Decode(); err != nil {
			return
		}
		_ = enc.Encode(sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("dirid")).
			Set("name", sproto.String("d")).
			Set("is_dir", sproto.Int(1)))
	})
	c := newClient(conn, "sess")

	node, err := c.Upload(testContext(t), "p", "n", 0, 1, content)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if node.FileID != "newf" {
		t.Errorf("node = %+v, want file_id newf", node)
	}
	if c.Broken() {
		t.Fatal("connection marked broken after a drained, errno:0 upload")
	}
	dir, err := c.Mkdir(testContext(t), "p", "d", 0, 1)
	if err != nil {
		t.Fatalf("Mkdir on the reused connection: %v", err)
	}
	if dir.FileID != "dirid" {
		t.Errorf("mkdir node = %+v, want file_id dirid", dir)
	}
}

func TestUploadErrnoZeroWithoutNodeFields(t *testing.T) {
	// A legitimate sparse success: the server confirms with errno == 0 but echoes no
	// node fields. This must still succeed (the errno IS the evidence) - only the
	// engine's modify branch then backfills the file id from the op. The fix rejects
	// only an ABSENT errno, never a present errno with missing node fields.
	conn := serveMock(t, serveUpload(func(t *testing.T, r, b *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	}))
	c := newClient(conn, "sess")

	node, err := c.Upload(testContext(t), "p", "n", 0, 1, []byte("data"))
	if err != nil {
		t.Fatalf("Upload err = %v, want nil for a sparse errno=0 success", err)
	}
	if node.FileID != "" {
		t.Errorf("node = %+v, want empty (no node fields echoed)", node)
	}
}

func TestUploadContextCanceled(t *testing.T) {
	// Cancelling ctx mid-turn must interrupt the blocking reply read on the upload
	// path, which wires watchContext/contextErr itself. The server reads both
	// frames (so the body write completes) but never replies.
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		dec := sproto.NewDecoder(server)
		_, _ = dec.Decode()                // frame 1: metadata
		_, _ = dec.Decode()                // frame 2: body
		_, _ = io.Copy(io.Discard, server) // never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.Upload(ctx, "p", "n", 0, 1, []byte("data"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestMkdir(t *testing.T) {
	var req *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		req = r
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("dirid")).
			Set("name", sproto.String("newdir")).
			Set("is_dir", sproto.Int(1))
	})

	node, err := c.Mkdir(testContext(t), "parent1", "newdir", 0, 1)
	if err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if v, _ := req.Get("action"); v != sproto.String("upload") {
		t.Errorf("action = %v, want upload", v)
	}
	if v, _ := req.Get("path"); v != sproto.String("id:parent1/newdir") {
		t.Errorf("path = %v, want id:parent1/newdir", v)
	}
	if v, _ := req.Get("is_dir"); v != sproto.Int(1) {
		t.Errorf("is_dir = %v, want 1", v)
	}
	// A mkdir request carries no body frame.
	if _, ok := req.Get("body-continue"); ok {
		t.Errorf("mkdir request unexpectedly carries body-continue")
	}
	if !node.IsDir || node.FileID != "dirid" || node.Name != "newdir" {
		t.Errorf("node = %+v, want dir dirid newdir", node)
	}
}

func TestMkdirServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	_, err := c.Mkdir(testContext(t), "p", "n", 0, 1)
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

// TestUploadWarnsOnAHashItDidNotSend pins that a reply reporting different stored content is
// at least visible.
//
// The reply's hash is what the engine records as ServerHash, so a disagreement means the server
// holds bytes other than the ones streamed - and nothing downstream notices: the baseline's
// LocalHash matches the disk, the next push compares the server's hash against that same
// recorded value, and the replicas stay quietly different until somebody downloads.
func TestUploadWarnsOnAHashItDidNotSend(t *testing.T) {
	content := []byte("what the client sent")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("name", sproto.String("a.txt")).
			Set("hash", sproto.String("ffffffffffffffffffffffffffffffff"))
	})
	var logbuf bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logbuf, nil))

	if _, err := c.Upload(testContext(t), "p", "a.txt", 100, 1, content); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	out := logbuf.String()
	if !strings.Contains(out, "different content hash") {
		t.Errorf("no warning about the mismatched hash:\n%s", out)
	}
	for _, want := range []string{md4.ContentHash(content), "ffffffffffffffffffffffffffffffff"} {
		if !strings.Contains(out, want) {
			t.Errorf("the warning does not name %q:\n%s", want, out)
		}
	}
}

// TestUploadSilentWhenTheHashAgrees pins the negative side: the ordinary reply logs nothing.
func TestUploadSilentWhenTheHashAgrees(t *testing.T) {
	content := []byte("what the client sent")
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("hash", sproto.String(md4.ContentHash(content)))
	})
	var logbuf bytes.Buffer
	c.log = slog.New(slog.NewTextHandler(&logbuf, nil))

	if _, err := c.Upload(testContext(t), "p", "a.txt", 100, 1, content); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if logbuf.Len() != 0 {
		t.Errorf("an agreeing reply logged:\n%s", logbuf.String())
	}
}
