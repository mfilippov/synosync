package actions

// This file implements the Phase-3 download action: fetching a file's bytes from
// the server while verifying their integrity. The reply is a bare PObject Map
// carrying the node metadata plus the content as a Binary value under "data". The
// content is streamed through the codec (never fully buffered, per the
// streaming rule in docs/INVARIANTS.md): a BinaryStream callback tees each payload chunk into the
// caller's io.Writer and an md4.New() hash at once. After the full reply is
// decoded the computed lowercase-hex MD4 is compared against the server-reported
// content hash; a mismatch is reported as ErrHashMismatch and success is withheld.

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// downloadInlineLimit caps how many content bytes the decoder buffers in memory
// before switching a download reply to the streaming path. Files at or below it
// are small enough to hold inline; anything larger is streamed straight to the
// caller's writer so a large file never materialises in memory.
const downloadInlineLimit = 4096

// Download fetches the content of the file identified by fileID, writing the bytes
// to w as they arrive and returning the node's metadata. The file is addressed by
// id:<file_id>. Content is streamed (large payloads never fully buffered) and its
// MD4 content hash is verified against the server-reported value.
//
// Because bytes are teed to w as they stream in, integrity is verified only after
// w has already received the full payload: on any non-nil error - including
// ErrHashMismatch - w may hold partial or unverified content, so the caller MUST
// treat a non-nil return as "discard what was written" (e.g. download to a staging
// path and promote it only on a nil error). Verification is skipped when the server
// reports no content hash (an empty "hash" field) - that skip is logged at Warn so
// it is observable; callers that require integrity must ensure the server always
// advertises one. The turn is bound by ctx so cancellation or a deadline interrupts
// the blocking I/O.
func (c *Client) Download(ctx context.Context, fileID string, rootNodeID, targetSyncID int64, w io.Writer) (Node, error) {
	if c.broken {
		return Node{}, ErrConnBroken
	}
	stop := netctx.Watch(ctx, c.conn.NetConn())
	defer stop()

	// The scoping fields are mandatory on a live DSM: (root_node_id, root_node_path)
	// pin the lookup to one view and max_id=0 disables the version upper bound the
	// server would otherwise enforce. A bare {"path": "id:…"} draws 32772.
	params := sproto.NewMap().
		Set("path", sproto.String("id:"+fileID)).
		Set("c2_offload", sproto.Int(0)).
		Set("file", sproto.NewMap().Set("offset", sproto.Int(0))).
		Set("force_current_version", sproto.Int(0)).
		Set("max_id", sproto.Int(0)).
		Set("root_node_id", sproto.Int(rootNodeID)).
		Set("root_node_path", sproto.String("/")).
		Set("sync_id", sproto.Int(0)).
		Set("target_sync_id", sproto.Int(targetSyncID))
	req := buildRequest("download", c.session, c.identity, params)
	if err := c.conn.WriteRequest(sproto.CmdAction, req); err != nil {
		c.broken = true
		return Node{}, fmt.Errorf("actions: write download request: %w", netctx.Err(ctx, err))
	}
	// A standalone download is one turn per file, so the no-hash skip is logged at
	// Warn: exactly one Warn per file, not a flood.
	return c.readContentReply(ctx, "download", w, 0, slog.LevelWarn, "path", "id:"+fileID)
}

// readContentReply is the shared streaming core of Download and ResumeDownload:
// after the caller has written its request, it decodes the reply frame for action,
// teeing the content Binary into w and an MD4 hash at once (large payloads
// streamed through BinaryStream, never fully buffered), validates that the reply's
// "data" field is exactly the binary that streamed, decodes the node metadata and
// verifies the computed MD4 against the server-reported content hash. warnAttrs
// are the caller's addressing facts (path, offset) prepended to the
// skipped-verification log's attributes.
//
// skipHashLevel is the slog level the no-content-hash skip is logged at. A
// standalone Download is one turn per file and passes slog.LevelWarn (one Warn per
// file). ResumeDownload runs once per chunk, so it passes Warn only for the first
// chunk (offset 0) and Debug thereafter - otherwise a multi-GiB hash-less transfer
// would emit thousands of Warn lines, one per chunk, rolling the whole rotating-log
// history; the whole-transfer skip is still surfaced once at Warn by
// the engine's resumeDownloadInto.
//
// The broken-flag discipline is preserved on the callers' behalf: a partial decode
// leaves the stream misaligned and poisons the Client, while every later
// validation error inspects only the fully-decoded reply, so the connection stays
// byte-aligned and reusable.
//
// The decode runs under an idle-progress watchdog (idle.go): the reply's length is
// server-controlled, so no FIXED deadline is safe here - but a reply that stops
// delivering bytes entirely for downloadIdleTimeout is a wedged peer and is failed
// as ErrTransferStalled rather than blocking the op (and, through drainBatch's
// join, the whole sync cycle) forever. Any transfer still making progress, however
// slow, re-arms the budget and runs to completion.
func (c *Client) readContentReply(ctx context.Context, action string, w io.Writer, startOffset int64, skipHashLevel slog.Level, warnAttrs ...any) (Node, error) {
	hash := md4.New()
	sink := io.MultiWriter(w, hash)
	var streamed bool
	var streamCount int
	var streamLen int64
	var streamWriteErr error

	guard := watchIdle(c.conn.NetConn(), downloadIdleTimeout)
	defer guard.stop()

	dec := c.conn.DecoderWith(guard.wrap)
	dec.MaxInlineBinary = downloadInlineLimit
	dec.BinaryStream = func(b sproto.Binary, payload io.Reader) error {
		streamed = true
		streamCount++
		streamLen = b.Length
		if streamWriteErr != nil {
			// An earlier chunk already failed: keep draining so the decoder can
			// finish reading the reply and the connection stays byte-aligned for
			// the next turn.
			_, err := io.Copy(io.Discard, payload)
			return err
		}
		if _, err := io.Copy(sink, payload); err != nil {
			// Don't abort the decode on a destination write failure - that would
			// leave the rest of this payload (and the fields after it) unread on
			// the connection, desyncing the next action. Record the error, drain
			// the remainder so the whole reply is consumed, and surface it once
			// decoding has completed.
			streamWriteErr = err
			_, _ = io.Copy(io.Discard, payload)
		}
		return nil
	}
	resp, err := dec.Decode()
	// Step over any unsolicited keep-alive the server interleaved ahead of the
	// content reply (see isKeepAlive); taking one as the reply would report a
	// missing data field on a perfectly good transfer.
	for i := 0; err == nil && isKeepAlive(resp) && i < maxSkippedKeepAlives; i++ {
		resp, err = dec.Decode()
	}
	// The bound is spent with a keep-alive still in hand: the content reply was never reached and
	// the server still owes it, so this connection is one reply behind for good. Fall into the
	// poisoning arm below rather than reporting a missing data field on a transfer that never
	// arrived - see errKeepAliveFlood for why a byte-aligned desync must still re-dial.
	if err == nil && isKeepAlive(resp) {
		err = errKeepAliveFlood
	}
	if err != nil {
		// A partial decode leaves the stream misaligned; refuse to reuse the conn.
		c.broken = true
		if guard.stalled() {
			// The watchdog expired the read deadline: the peer stopped delivering
			// bytes without closing the socket. Report it as the typed stall rather
			// than the generic I/O timeout (and ahead of netctx.Err, which would map
			// the deadline error onto the caller's context) so the engine's log names
			// the cause. The turn is transient: the poisoned connection is re-dialed
			// by the pool and the op stays queued.
			return Node{}, fmt.Errorf("actions: read %s reply: no bytes for %s: %w",
				action, guard.idle, ErrTransferStalled)
		}
		return Node{}, fmt.Errorf("actions: read %s reply: %w", action, netctx.Err(ctx, err))
	}

	// decodeReply (and the validation below) only inspect the fully-decoded reply,
	// so the connection stays byte-aligned and these errors do not poison the Client.
	reply, err := decodeReply(action, resp)
	if err != nil {
		return Node{}, err
	}
	if streamWriteErr != nil {
		return Node{}, fmt.Errorf("actions: write %s content: %w", action, streamWriteErr)
	}

	// A successful reply must carry the content as a Binary "data" field; validate
	// it unconditionally. A missing or wrongly-typed data field is a malformed
	// reply (ErrUnexpectedReply) rather than a silently-empty transfer - including
	// for zero-byte payloads, which still arrive as an empty Binary.
	// The content and the content facts ride in the reply's "file" sub-map
	// (file.data / file.hash / file.size); the flat "data" form is the fallback the
	// in-memory fakes speak. Looking only at the top level made every real download
	// fail as "missing data field".
	facts := reply
	if fr, ok := reply.Get("file"); ok {
		if fm, ok := fr.(*sproto.Map); ok {
			facts = fm
		}
	}
	raw, ok := facts.Get("data")
	if !ok {
		raw, ok = reply.Get("data")
	}
	if !ok {
		return Node{}, fmt.Errorf("actions: %s reply missing data field: %w", action, ErrUnexpectedReply)
	}
	b, ok := raw.(sproto.Binary)
	if !ok {
		return Node{}, fmt.Errorf("actions: %s data is %T, want binary: %w", action, raw, ErrUnexpectedReply)
	}
	if streamed {
		// The streaming callback fires for every oversized Binary regardless of its
		// key, so confirm that exactly one binary streamed and that it is this "data"
		// field (matching length) - otherwise a reply carrying some other large
		// binary could have been teed to w and reported as content.
		if streamCount != 1 || streamLen != b.Length {
			return Node{}, fmt.Errorf("actions: %s streamed %d binaries (len %d) but data is len %d: %w",
				action, streamCount, streamLen, b.Length, ErrUnexpectedReply)
		}
	} else {
		// Small (inline) payloads never reach the BinaryStream callback: their bytes
		// are buffered in Binary.Data, so feed them through the same sink.
		if _, err := sink.Write(b.Data); err != nil {
			return Node{}, fmt.Errorf("actions: write %s content: %w", action, err)
		}
	}

	node := decodeNode(reply)
	// decodeNode reads the catalog's file_hash/file_size names; a download reports
	// the same facts as plain hash/size inside the "file" sub-map, so fill them in
	// from there when the catalog names are absent.
	if node.Hash == "" {
		if h, ok := facts.String("hash"); ok {
			node.Hash = canonicalHash(h)
		}
	}
	if node.Size == 0 {
		if sz, ok := facts.Int("size"); ok {
			node.Size = sz
		}
	}
	got := hex.EncodeToString(hash.Sum(nil))
	// The server may advertise the content hash in either of two places: the map
	// "hash" field (decoded into node.Hash) or the content Binary's BinaryEx (0x43)
	// send_hash frame, which the codec decodes into b.SendHash. Prefer the map
	// field; fall back to send_hash so a server that carries the hash only via 0x43
	// framing is still verified rather than logged as unverifiable. Both are folded
	// to lowercase hex (canonicalHash), matching decodeNode and the locally computed
	// MD4, so the byte comparison below is casing-independent.
	want := node.Hash
	if want == "" && b.SendHash != "" {
		want = canonicalHash(b.SendHash)
	}
	if want == "" {
		// The server advertised no content hash (neither the map field nor a
		// send_hash frame), so there is nothing to verify the streamed bytes
		// against. This is not a hard error (the codec already validated the reply
		// shape), but it is a real integrity gap - surface it so a misconfigured or
		// hostile server silently dropping the hash is observable rather than passing
		// as clean content. The level is the caller's (Warn for a standalone
		// download / the first resume chunk, Debug for later resume chunks) so a
		// multi-chunk transfer logs one Warn, not one per chunk.
		c.log.Log(ctx, skipHashLevel, "actions: "+action+" reply carried no content hash; skipping MD4 verification",
			append(append([]any{}, warnAttrs...), "computed_md4", got, "bytes", b.Length)...)
		return node, nil
	}
	// The advertised hash describes the WHOLE node, so it can only be compared against these
	// bytes when these bytes ARE the whole node. For a resume CHUNK they are a slice of it, and
	// comparing the two failed every chunked transfer with ErrHashMismatch - including the
	// first chunk, so the verb only ever worked against a server that returns the entire
	// remainder in one reply. That is the shape the harness happened to measure; it was never
	// a guarantee, and docs/INTEGRATION.md records nothing about resume_download chunking.
	//
	// A single-reply transfer still verifies here, which is the common case and the one worth
	// catching at the turn. A genuinely chunked one is verified by whoever assembles it:
	// resumeDownloadInto hashes the whole reassembled file and compares it before the staged
	// copy is promoted, and warns on its own when there was no hash to compare against. The
	// skip is placed AFTER the no-hash branch above so a missing hash stays observable per
	// chunk - that is an integrity gap in the reply, not a consequence of chunking.
	if startOffset != 0 || (node.Size > 0 && b.Length != node.Size) {
		return node, nil
	}
	if got != want {
		return Node{}, fmt.Errorf("%w: computed %s, server reported %s", ErrHashMismatch, got, want)
	}
	return node, nil
}
