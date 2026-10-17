package actions

// This file implements the Phase-3 upload and mkdir actions: creating a file
// from a small in-memory payload, and creating a directory. Both are
// upload-shaped CmdAction requests addressed by id:<parent_id>/<name>.
//
// A file upload is a two-frame exchange: frame 1 is the metadata request
// carrying body-continue:1 and the payload's MD4 content hash; frame 2 is a
// bare PObject body frame (@proto envelope type "body") carrying the content
// bytes, written straight onto the connection with sproto.NewEncoder. Only the
// final reply is read. mkdir is a single metadata frame with is_dir:1 and no
// body, so it goes through the ordinary do turn.
//
// Upload here covers only files that fit in a single PObject frame (large-file
// resume lives in resume.go), so the caller passes the bytes directly.

import (
	"context"
	"fmt"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// Upload creates the file named name under the directory identified by parentID
// from content, returning the created node's metadata. The node is addressed by
// id:<parent_id>/<name>. The exchange is two frames: a metadata request
// (body-continue:1 plus the content's MD4 hash and size) followed by a bare
// PObject body frame carrying the bytes. The body frame is written concurrently
// with reading the reply so a server that rejects early (without draining the
// body) surfaces its typed error instead of deadlocking on write backpressure.
// The turn is bound by ctx so cancellation or a deadline interrupts the blocking
// I/O. A stale-base conflict is reported as ErrConflictVersion (non-fatal) so the
// caller can resync or keep-both later. On any interrupted or rejected turn the
// Client is left broken (ErrConnBroken) because the partially-written body
// desyncs the connection. Even the success path never blocks indefinitely on the
// body writer: the join is bounded by bodyJoinGrace, and a server that reports a
// committed write while leaving the body undrained is rejected as
// ErrUnexpectedReply with the connection poisoned (joinBodyFrame).
func (c *Client) Upload(ctx context.Context, parentID, name string, mtime int64, rootNodeID int64, content []byte) (Node, error) {
	if c.broken {
		return Node{}, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()

	hash := md4.ContentHash(content)
	params := uploadParams(parentID, name, mtime, rootNodeID)
	params.Set("@proto", protoHeader(1)).
		Set("file_type", sproto.String("file")).
		Set("is_dir", sproto.Int(0)).
		Set("exec_bit", referInt("exec_bit", 0)).
		Set("file", fileMeta(int64(len(content)), hash, nil))
	req := buildRequest("upload", c.session, c.identity, params)
	if err := c.conn.WriteRequest(sproto.CmdAction, req); err != nil {
		c.broken = true
		return Node{}, fmt.Errorf("actions: write upload request: %w", netctx.Err(ctx, err))
	}

	// Frame 2: the bare-PObject body frame. The "@proto" envelope marks it as a
	// body continuation; the bytes ride inline under "data".
	//
	// Write the body concurrently with reading the reply. A server that rejects on
	// the metadata alone may reply without draining the body, and the body is not
	// size-capped, so a sequential write-then-read would block on socket
	// backpressure once the buffers fill - hiding the server's typed error behind a
	// wedged write until the context deadline. Reading concurrently lets an early
	// reply through; the writer is then unblocked by closing the (now-desynced)
	// connection.
	// Frame 2 re-sends every metadata field verbatim and adds file.data. The
	// earlier form - a bare {"@proto":{"type":"body"},"data":…} - put the bytes at
	// the top level under the wrong key and omitted the metadata the server
	// re-parses, so no upload could ever land.
	bodyParams := uploadParams(parentID, name, mtime, rootNodeID)
	bodyParams.Set("@proto", protoBody()).
		Set("file_type", sproto.String("file")).
		Set("is_dir", sproto.Int(0)).
		Set("exec_bit", referInt("exec_bit", 0)).
		Set("file", fileMeta(int64(len(content)), hash, content))
	body := buildRequest("upload", c.session, c.identity, bodyParams)
	//
	// The idle watchdog is armed HERE, before the writer starts, and the writer reports
	// through it: no reply byte can arrive until the server has drained the whole body,
	// so a read-only guard would see the entire body transfer as silence and act as a
	// FIXED deadline on it - aborting every slow-link upload and re-pushing it from byte
	// zero forever. wrapWriter re-arms the budget per progressWriteChunk of body actually
	// handed to the kernel, so only a peer that stops draining (which stalls the write
	// too) fires it - down to the write-side floor rate documented on that constant.
	nc := c.conn.NetConn()
	guard := watchIdle(nc, actionIdleTimeout)
	bodyErr := make(chan error, 1)
	go func() { bodyErr <- sproto.NewEncoder(guard.wrapWriter(nc)).Encode(body) }()

	resp, oversize, stalled, err := c.readCappedReplyGuarded(guard)
	if err != nil {
		// The reply read failed (or was interrupted) before the body finished: the
		// connection is misaligned. Poison and close it - closing also unblocks the
		// body writer if it is wedged on backpressure - then join the writer.
		c.broken = true
		_ = c.conn.Close()
		<-bodyErr
		if stalled {
			return Node{}, stallError("upload", actionIdleTimeout)
		}
		return Node{}, fmt.Errorf("actions: read upload reply: %w", netctx.Err(ctx, err))
	}
	if oversize {
		// An upload reply carries only metadata, so an oversized inline Binary is
		// hostile or corrupt. readCappedReply drained it, but the concurrent body
		// writer may still be wedged (a server that replied without draining the
		// body), so the connection can't be trusted: poison and close to unblock the
		// writer, join it, and reject the reply.
		c.broken = true
		_ = c.conn.Close()
		<-bodyErr
		return Node{}, fmt.Errorf("actions: upload reply has oversized inline binary (>%d bytes): %w",
			maxReplyInlineBinary, ErrUnexpectedReply)
	}
	reply, derr := decodeReply("upload", resp)
	if derr != nil {
		// The server replied with an error (or a malformed frame) possibly before
		// draining the body, so the half-written body leaves the stream misaligned.
		// Poison and close to unblock the writer, then join it and surface the typed
		// error rather than the write failure.
		c.broken = true
		_ = c.conn.Close()
		<-bodyErr
		return Node{}, derr
	}
	// Require the reply to carry an errno, mirroring doMutate. A small-file Upload
	// is a mutating turn that reports nothing but success, so a present errno == 0 is
	// its only positive evidence the server actually committed the create/modify -
	// the node fields (file_id/hash) are unconfirmed against a live DSM (DESIGN §13),
	// so they are not proof. An errno-less reply (a flaky/non-conforming DSM that
	// answered {}) must NOT be recorded locally as a completed upload: the engine's
	// modify branch would backfill the file id from the op and mark the baseline
	// StateSynced against a write the server never made, diverging silently forever.
	//
	// This errno-presence check runs BEFORE joining the body writer, mirroring the
	// three close-before-join branches above (read-err/oversize/decode-err). An
	// errno-less reply is exactly the shape where a non-conforming server answered on
	// the metadata frame alone WITHOUT draining the body, so the concurrent body
	// writer may be wedged on socket/TLS send backpressure (a body over the send
	// buffer, ~1-4 MiB). Joining first would then block the worker forever when
	// limits.op_turn_timeout_s is disabled (the default) - no deadline forces the
	// socket, so the close that would unblock the writer is never reached. Poison
	// and close FIRST - the close unblocks the wedged writer - then
	// join it and reject the reply.
	//
	// Unlike doMutate (a single-frame turn), Upload wrote a second body frame. A
	// server that answered {} on the metadata alone may not have drained that body:
	// even our side is byte-aligned only once the body write is unblocked and joined,
	// and the SERVER's framing state is undecided regardless - it may read our body
	// frame as the next request, shifting every later turn by one reply. That is
	// unprovable from here, so poison and close the connection (one re-dial on an
	// already-non-conforming reply) rather than hand a possibly server-desynced socket
	// back to the pool - mirroring the decode-error branch above.
	// As in doMutate: the evidence is the result verdict, which a live DSM states
	// as "type":"response" with no "error" block rather than a literal errno key.
	if _, present, verr := errno(reply); verr != nil || !present {
		c.broken = true
		_ = c.conn.Close()
		<-bodyErr
		if verr != nil {
			return Node{}, verr
		}
		return Node{}, verdictlessError("upload", reply)
	}
	// An errno-bearing success reply means the server consumed the whole body, so the
	// writer has finished; a write error here is still fatal to the connection. The
	// join is nonetheless bounded (joinBodyFrame): a server that claims success while
	// leaving the body undrained must not wedge the worker forever either.
	if err := c.joinBodyFrame(ctx, "upload", bodyErr); err != nil {
		return Node{}, err
	}
	node := decodeNode(reply)
	c.noteUploadHashMismatch(ctx, "upload", node, hash)
	return node, nil
}

// Mkdir creates the directory named name under the directory identified by
// parentID, returning the created node's metadata. It is an upload-shaped
// request addressed by id:<parent_id>/<name> with is_dir:1 and no body frame,
// so it runs as an ordinary single-turn action. The turn is bound by ctx.
//
// Unlike Upload, Mkdir deliberately stays on the tolerant do (no errno-presence
// requirement): its engine consumer (applyMkdirOut) already requires a
// non-empty FileID as the create's positive evidence - an errno-less {} reply
// decodes to Node{FileID:""}, which the engine recovers via a QueryNode probe or
// leaves the op queued for retry, so it can never be recorded as a silent success.
func (c *Client) Mkdir(ctx context.Context, parentID, name string, mtime int64, rootNodeID int64) (Node, error) {
	// There is no "mkdir" action: a directory is created by the SAME upload verb
	// with file_type "dir", is_dir 1 and NO file sub-map - and it is still a
	// two-frame turn with an empty body continuation. The single-frame
	// {"path":…, "is_dir":1} form this used to send was rejected with 32769.
	return c.uploadDir(ctx, parentID, name, mtime, rootNodeID)
}

// uploadParams builds the metadata fields common to both upload frames.
func uploadParams(parentID, name string, mtime, rootNodeID int64) *sproto.Map {
	return sproto.NewMap().
		Set("conflict_policy", sproto.String("compare_mtime")).
		Set("max_id", sproto.Int(0)).
		Set("mtime", referInt("mtime", mtime)).
		Set("path", sproto.String("id:"+parentID+"/"+name)).
		Set("root_node_id", sproto.Int(rootNodeID)).
		Set("root_node_path", sproto.String("/")).
		Set("sync_id", sproto.Int(0))
}

// referInt renders the {"<key>": v, "refer": 0} shape the server uses for the
// scalar sub-maps mtime and exec_bit.
func referInt(key string, v int64) *sproto.Map {
	return sproto.NewMap().Set(key, sproto.Int(v)).Set("refer", sproto.Int(0))
}

// fileMeta builds the "file" sub-map. data is nil on the metadata frame and the
// content on the body frame.
func fileMeta(size int64, hash string, data []byte) *sproto.Map {
	m := sproto.NewMap().
		Set("hash", sproto.String(hash)).
		Set("is_delta", sproto.Int(0)).
		Set("real_size", sproto.Int(size)).
		Set("refer", sproto.Int(0)).
		Set("size", sproto.Int(size))
	if data != nil {
		m.Set("data", sproto.Binary{Data: data})
	}
	return m
}

// protoHeader builds the @proto envelope of a request's header frame.
func protoHeader(bodyContinue int64) *sproto.Map {
	return sproto.NewMap().
		Set("body-continue", sproto.Int(bodyContinue)).
		Set("type", sproto.String("header")).
		Set("version", sproto.NewMap().Set("major", sproto.Int(7)).Set("minor", sproto.Int(0)))
}

// protoBody builds the @proto envelope of a continuation (content) frame.
func protoBody() *sproto.Map {
	return sproto.NewMap().
		Set("body-continue", sproto.Int(0)).
		Set("type", sproto.String("body"))
}

// uploadDir performs the two-frame upload turn that creates a directory: the
// metadata frame carries file_type "dir" / is_dir 1 and NO file sub-map, and the
// continuation frame is the same metadata with the body @proto envelope and no
// content. It mirrors Upload's concurrent write/read discipline for the same
// reason - a server that rejects on the metadata alone must surface its typed
// error rather than deadlock the turn on write backpressure.
func (c *Client) uploadDir(ctx context.Context, parentID, name string, mtime, rootNodeID int64) (Node, error) {
	if c.broken {
		return Node{}, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()

	meta := func(proto *sproto.Map) *sproto.Map {
		p := uploadParams(parentID, name, mtime, rootNodeID)
		p.Set("@proto", proto).
			Set("file_type", sproto.String("dir")).
			Set("is_dir", sproto.Int(1)).
			Set("exec_bit", referInt("exec_bit", 1))
		return buildRequest("upload", c.session, c.identity, p)
	}

	if err := c.conn.WriteRequest(sproto.CmdAction, meta(protoHeader(1))); err != nil {
		c.broken = true
		return Node{}, fmt.Errorf("actions: write mkdir request: %w", netctx.Err(ctx, err))
	}

	guard := watchIdle(c.conn.NetConn(), actionIdleTimeout)
	bodyErr := make(chan error, 1)
	go func() {
		bodyErr <- sproto.NewEncoder(guard.wrapWriter(c.conn.NetConn())).Encode(meta(protoBody()))
	}()

	resp, oversize, stalled, err := c.readCappedReplyGuarded(guard)
	if err != nil {
		c.broken = true
		c.conn.Close()
		<-bodyErr
		if stalled {
			return Node{}, stallError("mkdir", actionIdleTimeout)
		}
		return Node{}, fmt.Errorf("actions: read mkdir reply: %w", netctx.Err(ctx, err))
	}
	if oversize {
		c.broken = true
		c.conn.Close()
		<-bodyErr
		return Node{}, fmt.Errorf("actions: mkdir reply has oversized inline binary: %w", ErrUnexpectedReply)
	}
	if err := c.joinBodyFrame(ctx, "mkdir", bodyErr); err != nil {
		return Node{}, err
	}
	m, err := decodeReply("mkdir", resp)
	if err != nil {
		return Node{}, err
	}
	return decodeNode(m), nil
}

// noteUploadHashMismatch reports a stored-content hash that disagrees with the one just sent.
//
// The reply's node hash is what the engine records as ServerHash, so a disagreement means the
// server holds bytes other than the ones this client streamed - and nothing downstream would
// ever notice: the baseline's LocalHash matches the disk, the next push compares the server's
// hash against that same recorded value, and the two replicas stay quietly different until
// somebody downloads the file.
//
// A WARN rather than a refusal, deliberately. The failure directions are wildly asymmetric: a
// server whose reply hash differs benignly - a different digest, a different encoding of the
// same one - would have EVERY upload fail forever, which kills syncing outright, while the
// corruption this catches is rare and leaves the operator a line naming both values. Turning it
// into an error needs the live measurement first; docs/INTEGRATION.md pins what `list` and
// `query_node` report, not what an upload reply carries back.
func (c *Client) noteUploadHashMismatch(ctx context.Context, action string, node Node, sent string) {
	if sent == "" || node.Hash == "" || node.Hash == sent {
		return
	}
	c.log.WarnContext(ctx, "actions: "+action+" reply reports a different content hash than was sent; "+
		"the server may hold different bytes than this client streamed",
		"name", node.Name, "file_id", node.FileID, "sent_md4", sent, "server_md4", node.Hash)
}
