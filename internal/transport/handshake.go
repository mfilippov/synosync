package transport

// This file implements the two unencrypted exchanges that precede the TLS
// upgrade: the TEST_CONN probe (a throwaway connection that yields the server's
// ds_id) and the encrypt_channel request (which announces the client agent and
// the restore id before the channel is encrypted). Session.Connect (session.go)
// chains encrypt_channel into the in-place TLS upgrade.

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// Dialer abstracts establishing a raw TCP connection so tests can supply a
// loopback listener while production uses net.Dialer.
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// defaultDialer is the standard TCP dialer used when a caller supplies none.
var defaultDialer Dialer = &net.Dialer{}

// maxTestConnReply bounds how many bytes a TEST_CONN reply may occupy. The
// exchange happens before TLS, so the peer is unauthenticated; a legitimate
// reply holds only a few tiny TLV sections, well under this cap. Bounding the
// read keeps a hostile endpoint from streaming data until memory is exhausted,
// since the per-section limits in DecodeTestConnReply only apply after the
// whole frame is in memory.
const maxTestConnReply = 64 << 10 // 64 KiB

// maxEncryptChannelReply bounds the encrypt_channel reply. Like TEST_CONN this
// exchange happens before TLS, so the peer is unauthenticated and a hostile
// endpoint could stream a large PObject to exhaust memory before cert pinning
// occurs. Unlike TEST_CONN the socket is not read to EOF - it is upgraded to TLS
// in place - so the reply cannot be buffered and length-checked; it is decoded
// as a stream under this read cap instead. A legitimate reply is a tiny map
// (agent, alive), well under 64 KiB.
const maxEncryptChannelReply = 64 << 10 // 64 KiB

// cappedReader bounds the total bytes that may be read through it. Once the
// budget is exhausted it returns an error naming the cap rather than the
// io.ErrUnexpectedEOF a bare io.LimitReader would surface mid-decode, so an
// over-long pre-TLS reply is reported as the policy violation it is. It does not
// read ahead, so wrapping the socket for a single decode leaves no buffered
// plaintext bytes for the following TLS upgrade to inherit.
type cappedReader struct {
	r    io.Reader
	left int64
	cap  int64
}

func (cr *cappedReader) Read(p []byte) (int, error) {
	if cr.left <= 0 {
		return 0, fmt.Errorf("transport: encrypt_channel reply exceeds %d bytes", cr.cap)
	}
	if int64(len(p)) > cr.left {
		p = p[:cr.left]
	}
	n, err := cr.r.Read(p)
	cr.left -= int64(n)
	return n, err
}

// TestConn opens a throwaway TCP connection to addr, sends a TEST_CONN probe and
// returns the decoded legacy reply - chiefly its DsID, which seeds the
// encrypt_channel restore id. The connection is closed before returning.
func TestConn(ctx context.Context, addr string) (sproto.TestConnReply, error) {
	return testConn(ctx, defaultDialer, addr)
}

// testConn is the dialer-injectable core of TestConn.
func testConn(ctx context.Context, d Dialer, addr string) (sproto.TestConnReply, error) {
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return sproto.TestConnReply{}, fmt.Errorf("transport: dial %s: %w", addr, err)
	}
	defer conn.Close()
	stop := netctx.Watch(ctx, conn)
	defer stop()
	if _, err := conn.Write(sproto.TestConnRequest()); err != nil {
		return sproto.TestConnReply{}, fmt.Errorf("transport: write TEST_CONN: %w", netctx.Err(ctx, err))
	}
	// The legacy reply is read to EOF: TEST_CONN runs on its own short-lived
	// connection and the server closes its write side after the frame. The read
	// is bounded so an unauthenticated peer cannot stream until memory is
	// exhausted; an over-long reply is rejected rather than buffered.
	data, err := io.ReadAll(io.LimitReader(conn, maxTestConnReply+1))
	if err != nil {
		return sproto.TestConnReply{}, fmt.Errorf("transport: read TEST_CONN reply: %w", netctx.Err(ctx, err))
	}
	if len(data) > maxTestConnReply {
		return sproto.TestConnReply{}, fmt.Errorf("transport: TEST_CONN reply exceeds %d bytes", maxTestConnReply)
	}
	// The TEST_CONN reply is one of the few server frames that DOES carry the
	// 8-byte header (post-TLS replies are bare PObjects). Validate and strip it:
	// feeding the header into the TLV decoder made it read the magic's first byte
	// as a section tag and fail with "unknown legacy section tag 0x25" - which is
	// exactly how a live DSM's perfectly good reply was rejected.
	var hdr sproto.Header
	if err := hdr.UnmarshalBinary(data); err != nil {
		return sproto.TestConnReply{}, fmt.Errorf("transport: TEST_CONN reply header: %w", err)
	}
	if hdr.Cmd != sproto.CmdTestConn {
		return sproto.TestConnReply{}, fmt.Errorf("transport: TEST_CONN reply carries cmd 0x%02x, want 0x%02x", hdr.Cmd, sproto.CmdTestConn)
	}
	reply, err := sproto.DecodeTestConnReply(data[sproto.HeaderSize:])
	if err != nil {
		return sproto.TestConnReply{}, fmt.Errorf("transport: decode TEST_CONN reply: %w", err)
	}
	return reply, nil
}

// EncryptChannelRequest builds the encrypt_channel PObject sent on the fresh TCP
// connection just before the TLS upgrade. It carries the action name, the client
// agent and the server restore id (the ds_id learned from TEST_CONN).
//
// The "action" key is mandatory. The server dispatches on it, not on the header's
// cmd byte, so a body without it is discarded and the connection closed with no
// reply - which is exactly how this request failed against a live DSM before the
// key was added. A capture of the real Drive Client sends action first, then
// agent, then (on a reconnect) server_id.
func EncryptChannelRequest(agent sproto.Value, dsID string) *sproto.Map {
	m := sproto.NewMap().Set("action", sproto.String("encrypt_channel"))
	if agent != nil {
		m.Set("agent", agent)
	}
	m.Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(dsID)))
	return m
}

// exchangeEncryptChannel sends the encrypt_channel request over c and reads the
// bare PObject reply, returning the server's "alive" value. It runs before the
// TLS upgrade; the channel is still plaintext.
func exchangeEncryptChannel(c *sproto.Conn, agent sproto.Value, dsID string) (int64, error) {
	if err := c.WriteRequest(sproto.CmdRequestSSL, EncryptChannelRequest(agent, dsID)); err != nil {
		return 0, fmt.Errorf("transport: write encrypt_channel: %w", err)
	}
	// Cap the unauthenticated pre-TLS reply: decode it as a bounded stream so a
	// hostile endpoint cannot flood the channel until memory is exhausted. The cap
	// reads straight from the socket without buffering ahead, so the in-place TLS
	// upgrade that follows inherits no plaintext bytes.
	capped := &cappedReader{r: c.NetConn(), left: maxEncryptChannelReply, cap: maxEncryptChannelReply}
	resp, err := sproto.ReadResponse(capped)
	if err != nil {
		return 0, fmt.Errorf("transport: read encrypt_channel reply: %w", err)
	}
	m, ok := resp.(*sproto.Map)
	if !ok {
		return 0, fmt.Errorf("transport: encrypt_channel reply is %T, want map", resp)
	}
	// A legitimate reply always carries an integer "alive". Validate its presence
	// rather than defaulting a missing/malformed value silently to 0: an absent
	// alive signals a non-conforming (or hostile) endpoint, so the handshake fails
	// loudly instead of producing a meaningless Session.Alive of 0.
	alive, ok := m.Int("alive")
	if !ok {
		return 0, fmt.Errorf("transport: encrypt_channel reply missing integer \"alive\"")
	}
	return alive, nil
}
