package actions

// This file implements the Phase-4 large-file resume transfer actions:
// ResumeGetToken (obtain a resume token for a transfer target), ResumeUpload
// (send a large upload) and ResumeDownload (fetch one offset-based chunk of a
// large download).
//
// There is no ResumeStatus action and no committed-offset decoding: resume_upload
// carries the WHOLE file in one two-frame turn and takes no offset at all
// (measured 31.07.2026, docs/INTEGRATION.md), so the reply's own verdict is the
// completion signal - see the whole-file note on ResumeUpload below. Only the
// DOWNLOAD side is offset-based.
//
// resume_get_token is an ordinary single-frame CmdAction turn, so it goes through
// Client.do (which injects "action"/"session", reads the bare PObject reply, and
// maps any non-zero errno - including an invalid/expired token, CodeResumeToken →
// ErrResumeToken - to a typed error). The two-frame chunked upload and the
// streamed download drive the connection directly, mirroring Upload/Download.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// ResumeRequest describes the large-file transfer a resume token is requested
// for. A token is scoped to (session, view) rather than to one transfer, so the
// request names only the view - the same token serves every large transfer in it,
// in either direction, and is refreshed on reconnect.
type ResumeRequest struct {
	// ViewID is the view the token is scoped to.
	ViewID int64
	// RootNodeID is that view's root node id (see View.RootNodeID).
	RootNodeID int64
}

// ResumeGetToken requests a resume token for the view's large-file transfers. The
// reply is decoded into a ResumeToken; a reply that carries no token is treated as
// malformed (ErrUnexpectedReply). The turn is bound by ctx.
func (c *Client) ResumeGetToken(ctx context.Context, req ResumeRequest) (ResumeToken, error) {
	// The token is scoped to (session, view) - not to a particular transfer - so the
	// request names the view, not the file. A live DSM also answers the older
	// path/size form, but this is the shape the real client sends and the one the
	// token's semantics match: one token serves many transfers in the view.
	params := sproto.NewMap().
		Set("view_id", sproto.Int(req.ViewID)).
		Set("root_node_id", sproto.Int(req.RootNodeID)).
		Set("root_node_path", sproto.String("/"))
	reply, err := c.do(ctx, "resume_get_token", params)
	if err != nil {
		return ResumeToken{}, err
	}
	tok := decodeResumeToken(reply)
	if tok.Token == "" {
		return ResumeToken{}, fmt.Errorf("actions: resume_get_token reply missing token: %w", ErrUnexpectedReply)
	}
	return tok, nil
}

// ResumeUploadSpec describes the whole file a resume upload transfers. The server
// wants the COMPLETE file's facts on both frames - this is not a per-chunk
// descriptor.
type ResumeUploadSpec struct {
	// ParentID is the directory the file is created under.
	ParentID string
	// Name is the leaf name within ParentID.
	Name string
	// MTime is the file's modification time as a unix timestamp.
	MTime int64
	// RootNodeID is the view root's integer node id (see Identity/View.RootNodeID).
	RootNodeID int64
	// Size is the file's total content length in bytes.
	Size int64
	// Hash is the whole file's MD4 content hash (lowercase hex).
	Hash string
}

// ResumeUpload transfers a large file under a resume token, returning the created
// node. The exchange is `upload`'s two frames plus `resume_token`: a metadata frame
// carrying the WHOLE file's hash and size, then a bare-PObject frame whose
// `file.data` holds the entire content, streamed from body so a large file is never
// buffered in memory.
//
// There is NO sproto-level chunking and no offset in this verb - a live DSM takes a
// 100 MiB file as one content frame, and the earlier per-chunk/offset model here
// (with its ResumeStatus loop) simply is not the protocol. A failed transfer
// therefore restarts from byte zero; only resume_download carries a resume offset.
//
// The token comes from ResumeGetToken and is mandatory: without it the server
// answers 45057. The `upload` + `dry_run:1` negotiation the real client performs
// first is advisory - the transfer succeeds without it - so it is not issued.
//
// The body frame is written concurrently with reading the reply so a server that
// rejects on the metadata alone (without draining the body) surfaces its typed error
// instead of deadlocking on write backpressure; every failure path poisons and
// closes the connection, since a half-written body leaves the stream misaligned.
func (c *Client) ResumeUpload(ctx context.Context, token string, spec ResumeUploadSpec, body io.Reader) (Node, error) {
	if c.broken {
		return Node{}, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()

	meta := func(proto *sproto.Map, content io.Reader) *sproto.Map {
		m := uploadParams(spec.ParentID, spec.Name, spec.MTime, spec.RootNodeID)
		m.Set("@proto", proto).
			Set("file_type", sproto.String("file")).
			Set("is_dir", sproto.Int(0)).
			Set("exec_bit", referInt("exec_bit", 0)).
			Set("resume_token", sproto.String(token))
		f := sproto.NewMap().
			Set("hash", sproto.String(spec.Hash)).
			Set("is_delta", sproto.Int(0)).
			Set("real_size", sproto.Int(spec.Size)).
			Set("refer", sproto.Int(0)).
			Set("size", sproto.Int(spec.Size))
		if content != nil {
			f.Set("data", sproto.Binary{Length: spec.Size, Reader: content})
		}
		m.Set("file", f)
		return buildRequest("resume_upload", c.session, c.identity, m)
	}

	if err := c.conn.WriteRequest(sproto.CmdAction, meta(protoHeader(1), nil)); err != nil {
		c.broken = true
		return Node{}, fmt.Errorf("actions: write resume_upload request: %w", netctx.Err(ctx, err))
	}

	// The idle watchdog is armed before the writer starts and the body write reports
	// through it (idleGuard.wrapWriter): no reply byte arrives until the server has
	// drained the content, so a read-only guard would turn the budget into a fixed
	// deadline on the whole transfer and abort every slow-link upload.
	nc := c.conn.NetConn()
	guard := watchIdle(nc, actionIdleTimeout)
	bodyErr := make(chan error, 1)
	// closedBy records which side tore the connection down first: 1 the body writer, 2 the
	// reader. It decides which error is the CAUSE, since whichever side loses the race then
	// fails with a consequential "use of closed network connection".
	const (
		closedByBody   = 1
		closedByReader = 2
	)
	var closedBy atomic.Int32
	go func() {
		err := sproto.NewEncoder(guard.wrapWriter(nc)).Encode(meta(protoBody(), body))
		if err != nil && closedBy.CompareAndSwap(0, closedByBody) {
			// The body could not be written - the local file shrank, or the read failed - so
			// the server is waiting for bytes that will never arrive and will never reply.
			// Nothing was going to move on this connection in either direction, and the only
			// thing that ended the turn was the idle budget: five minutes per attempt, on a
			// failure that was known immediately, with a worker lease held throughout and the
			// op re-wedging it on the next drain. Close here so the blocked read returns at
			// once and the real reason is what surfaces.
			_ = c.conn.Close()
		}
		bodyErr <- err
	}()

	fail := func(err error) (Node, error) {
		c.broken = true
		closedBy.CompareAndSwap(0, closedByReader)
		_ = c.conn.Close()
		berr := <-bodyErr
		if berr != nil && closedBy.Load() == closedByBody {
			// The writer failed first and closed the connection; the read error is the
			// consequence, so report the cause.
			return Node{}, fmt.Errorf("actions: resume_upload body: %w", berr)
		}
		return Node{}, err
	}

	resp, oversize, stalled, err := c.readCappedReplyGuarded(guard)
	if err != nil {
		if stalled {
			return fail(stallError("resume_upload", actionIdleTimeout))
		}
		return fail(fmt.Errorf("actions: read resume_upload reply: %w", netctx.Err(ctx, err)))
	}
	if oversize {
		return fail(fmt.Errorf("actions: resume_upload reply has oversized inline binary (>%d bytes): %w",
			maxReplyInlineBinary, ErrUnexpectedReply))
	}
	reply, derr := decodeReply("resume_upload", resp)
	if derr != nil {
		return fail(derr)
	}
	// Success evidence is the result verdict, exactly as for Upload: a live DSM marks
	// the frame type:"response" with no error block rather than carrying an errno.
	if _, present, verr := errno(reply); verr != nil || !present {
		if verr != nil {
			return fail(verr)
		}
		return fail(verdictlessError("resume_upload", reply))
	}
	if err := c.joinBodyFrame(ctx, "resume_upload", bodyErr); err != nil {
		return Node{}, err
	}
	node := decodeNode(reply)
	c.noteUploadHashMismatch(ctx, "resume_upload", node, spec.Hash)
	return node, nil
}

// ResumeDownload fetches one offset-based chunk of a large file for the transfer
// identified by token, writing the returned bytes to w as they arrive and returning
// the chunk's node metadata. The file is addressed by id:<fileID>; the resume token
// and the byte offset to resume from are threaded through the request. The reply's
// content is streamed exactly like Download (large payloads never fully buffered)
// and the server-advertised MD4 content hash of the returned chunk is verified
// against the bytes that streamed through.
//
// As in Download the bytes are teed to w as they stream in, so integrity is verified
// only after w has already received the full payload: on any non-nil error -
// including ErrHashMismatch - w may hold partial or unverified content, so the caller
// MUST treat a non-nil return as "discard what was written" (e.g. download to a
// staging path and promote it only on a nil error). Verification is skipped when the
// server reports no content hash (an empty "hash" field). An invalid/expired token is
// mapped to ErrResumeToken so the caller can re-token and restart from the committed
// offset. The turn is bound by ctx; an interrupted decode poisons the Client
// (ErrConnBroken).
func (c *Client) ResumeDownload(ctx context.Context, token, fileID string, rootNodeID, targetSyncID, offset int64, w io.Writer) (Node, error) {
	if c.broken {
		return Node{}, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()

	// resume_download is `download` plus resume_token, with the resume offset inside
	// the "file" sub-map - not a top-level "offset". The scoping fields
	// (root_node_id / root_node_path / max_id) are as mandatory here as on download.
	params := sproto.NewMap().
		Set("path", sproto.String("id:"+fileID)).
		Set("resume_token", sproto.String(token)).
		Set("c2_offload", sproto.Int(0)).
		Set("file", sproto.NewMap().Set("offset", sproto.Int(offset))).
		Set("force_current_version", sproto.Int(0)).
		Set("max_id", sproto.Int(0)).
		Set("root_node_id", sproto.Int(rootNodeID)).
		Set("root_node_path", sproto.String("/")).
		Set("sync_id", sproto.Int(0)).
		Set("target_sync_id", sproto.Int(targetSyncID))
	req := buildRequest("resume_download", c.session, c.identity, params)
	if err := c.conn.WriteRequest(sproto.CmdAction, req); err != nil {
		c.broken = true
		return Node{}, fmt.Errorf("actions: write resume_download request: %w", netctx.Err(ctx, err))
	}
	// The streamed reply (tee/hash sink, data-field validation, MD4 verification)
	// is identical to Download's, so it is decoded by the shared readContentReply.
	// A hash-less transfer logs the skipped-verification note once (the first chunk,
	// offset 0, at Warn); every later chunk demotes it to Debug so a multi-GiB
	// transfer emits one Warn, not one per chunk. The whole-transfer
	// skip is also Warned once by the engine's resumeDownloadInto.
	skipLevel := slog.LevelDebug
	if offset == 0 {
		skipLevel = slog.LevelWarn
	}
	return c.readContentReply(ctx, "resume_download", w, offset, skipLevel, "path", "id:"+fileID, "offset", offset)
}
