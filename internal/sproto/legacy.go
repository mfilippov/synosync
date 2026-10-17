package sproto

// This file decodes the "legacy" frame the server returns to a TEST_CONN probe.
// Unlike every message after the handshake (the PObject codec in pstream.go),
// the TEST_CONN reply is an older tag-length-value frame: a flat sequence of
// sections, each introduced by a 1-byte SPROTO_ID tag. Fixed-width fields carry
// 1/2/4/8 bytes of LITTLE-endian value; variable-length fields carry a
// BIG-endian uint16 length prefix followed by their raw bytes. The endianness
// asymmetry is Synology's, not a typo.
//
// The body is preceded on the wire by the ordinary 8-byte frame header
// (header.go); callers strip it before calling here.
//
// We only need a few fields from it - chiefly the DiskStation serial (ds_id),
// which later becomes the encrypt_channel restore_id. The decoder is deliberately
// strict and bounded: an unknown tag or an out-of-range length is a malformed
// frame, not something to skip, so a hostile or corrupt reply turns into a
// recoverable error rather than a desync or unbounded read.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Legacy-TLV section tags (SPROTO_ID) appearing in a TEST_CONN reply. These are
// the values a live DSM 7.x / Drive Server 4.0.3 actually sends; an earlier
// revision guessed 0x02/0x03/0x04 for the last three and read RES as a 4-byte
// field, so the real reply - `01 01 1c 46 1b 00 20 <ds_id> 1d 00 02 2d 2d` -
// failed to decode.
const (
	legacyTagRes          = 0x01 // result/status code (fixed, 1 byte; 1 = success)
	legacyTagSerialNumber = 0x1b // DiskStation serial = ds_id (vardata)
	legacyTagProtoVersion = 0x1c // protocol version (fixed, 1 byte; 0x46)
	legacyTagServerID     = 0x1d // server identifier (vardata; "--" on modern DSM)
)

// legacySuccess is the RES value a healthy TEST_CONN reply carries. The legacy
// frame reports success as 1, unlike the post-TLS action layer where errno 0
// means success - so callers must not compare TestConnReply.Result against 0.
const legacySuccess = 1

// maxLegacySections bounds how many TLV sections a single TEST_CONN reply may
// carry. Real replies hold only a handful; the cap turns a corrupt frame that
// never ends into a recoverable error instead of an unbounded loop.
const maxLegacySections = 64

// maxLegacyVardata bounds the byte length of a single variable-length section.
// The uint16 prefix already limits it to 65535, but legitimate values here are
// tiny (a 32-hex serial, a short server id), so a tighter cap rejects absurd
// lengths early.
const maxLegacyVardata = 4096

// ErrLegacyTruncated is returned when a TEST_CONN reply ends in the middle of a
// section. Callers can match it with errors.Is.
var ErrLegacyTruncated = errors.New("sproto: truncated legacy frame")

// TestConnReply holds the fields decoded from a TEST_CONN response. Only DsID is
// required by the handshake; the rest are surfaced for diagnostics.
type TestConnReply struct {
	// Result is the server's status code. The legacy frame reports success as
	// 1 (legacySuccess), not 0 - see Succeeded.
	Result int32
	// ProtoVersion is the server-advertised legacy protocol version (0x46).
	ProtoVersion int32
	// DsID is the DiskStation serial number, used as the encrypt_channel
	// restore_id. Typically a 32-character lowercase-hex string.
	DsID string
	// ServerID is an opaque server identifier, when present. Modern DSM sends
	// the placeholder "--".
	ServerID string
}

// Succeeded reports whether the reply carries the legacy success code. It exists
// so callers never hand-roll the comparison against the wrong sentinel: unlike
// the post-TLS action layer, where 0 means success, the legacy frame reports
// success as 1.
func (r TestConnReply) Succeeded() bool { return r.Result == legacySuccess }

// TestConnRequest returns the wire bytes of a TEST_CONN probe: just the 8-byte
// frame header carrying CmdTestConn, with no body.
func TestConnRequest() []byte {
	buf, _ := Header{Cmd: CmdTestConn}.MarshalBinary()
	return buf
}

// DecodeTestConnReply parses the legacy TLV body of a TEST_CONN response. The
// caller must pass the body ALONE - the 8-byte frame header the server prefixes
// is not part of it. It reads sections until the input is exhausted, rejecting
// unknown tags, oversized values and truncated frames.
func DecodeTestConnReply(data []byte) (TestConnReply, error) {
	var reply TestConnReply
	r := newByteReader(data)
	for sections := 0; ; sections++ {
		tag, err := r.readByte()
		if errors.Is(err, io.EOF) {
			return reply, nil
		}
		if err != nil {
			return TestConnReply{}, err
		}
		// Bound the section count only once a section actually remains to be
		// read: maxLegacySections is the number of sections a reply MAY carry, so
		// an exactly-maxLegacySections frame - whose next readByte returns EOF -
		// is valid and must decode cleanly. Checking before the EOF probe rejected
		// that boundary case (an off-by-one against the documented cap). A genuine
		// (maxLegacySections+1)-th section trips this before it is processed.
		if sections >= maxLegacySections {
			return TestConnReply{}, fmt.Errorf("sproto: legacy section count exceeds %d", maxLegacySections)
		}
		switch tag {
		case legacyTagRes:
			v, err := r.readFixed(1)
			if err != nil {
				return TestConnReply{}, err
			}
			reply.Result = v
		case legacyTagProtoVersion:
			v, err := r.readFixed(1)
			if err != nil {
				return TestConnReply{}, err
			}
			reply.ProtoVersion = v
		case legacyTagSerialNumber:
			v, err := r.readVardata()
			if err != nil {
				return TestConnReply{}, err
			}
			reply.DsID = v
		case legacyTagServerID:
			v, err := r.readVardata()
			if err != nil {
				return TestConnReply{}, err
			}
			reply.ServerID = v
		default:
			return TestConnReply{}, fmt.Errorf("sproto: unknown legacy section tag 0x%02x", tag)
		}
	}
}

// byteReader is a tiny cursor over an in-memory legacy frame.
type byteReader struct {
	buf []byte
	pos int
}

func newByteReader(buf []byte) *byteReader { return &byteReader{buf: buf} }

// readByte returns the next byte, or io.EOF if the cursor is at the end.
func (r *byteReader) readByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, io.EOF
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// take returns the next n bytes, or ErrLegacyTruncated if fewer remain.
func (r *byteReader) take(n int) ([]byte, error) {
	if n > len(r.buf)-r.pos {
		return nil, fmt.Errorf("%w: want %d bytes, have %d", ErrLegacyTruncated, n, len(r.buf)-r.pos)
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

// readFixed reads a fixed-width little-endian integer of n bytes (n ∈ {1,2,4}).
// Legacy fixed fields are little-endian, unlike the big-endian length prefix of
// a vardata section.
func (r *byteReader) readFixed(n int) (int32, error) {
	b, err := r.take(n)
	if err != nil {
		return 0, err
	}
	var v uint32
	for i := n - 1; i >= 0; i-- {
		v = v<<8 | uint32(b[i])
	}
	return int32(v), nil // #nosec G115 -- two's-complement reinterpretation of the decoded value
}

// readVardata reads a big-endian uint16 length prefix and that many bytes.
func (r *byteReader) readVardata() (string, error) {
	b, err := r.take(2)
	if err != nil {
		return "", err
	}
	n := int(binary.BigEndian.Uint16(b))
	if n > maxLegacyVardata {
		return "", fmt.Errorf("sproto: legacy section length %d exceeds %d", n, maxLegacyVardata)
	}
	body, err := r.take(n)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
