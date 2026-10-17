// Package sproto implements the Synology Drive wire protocol ("sproto"): the
// fixed 8-byte frame header, the recursive PStream/PObject streaming codec and
// a thin connection type with TLS upgrade.
//
// This file covers the frame header. Every client request is prefixed with an
// 8-byte, big-endian header laid out as four fields:
//
//	offset 0..3 : magic   uint32  (always HeaderMagic)
//	offset 4    : version uint8   (always HeaderVersion, 0x46)
//	offset 5    : cmd     uint8
//	offset 6..7 : pkt_len uint16  (payload length HINT; observed 0 in practice)
//
// The version and cmd fields are ONE BYTE EACH - not the two-byte pair an
// earlier revision of this file assumed. A live DSM 7.x / Drive Server 4.0.3
// stand settled it: with the two-byte layout the client's `00 46 00 0b` was read
// by the server as version=0x00 (invalid), and every command - TEST_CONN,
// encrypt_channel, even a bogus one - drew the same canned 10-byte rejection
// frame `25 52 18 14 46 <echo> 00 00 01 0a`. With version=0x46 at offset 4 and
// cmd at offset 5 the server answers TEST_CONN with the real ds_id.
//
// pkt_len is a hint only: the server sends 0 and the body is self-delimiting
// (Legacy TLV runs to end-of-frame, PStream carries its own end markers), so
// neither side may use it for framing.
//
// Server responses after the TLS upgrade are "bare" PObjects and carry no
// header; that case is handled by the codec, not here. The pre-TLS TEST_CONN
// reply DOES carry this header, followed by Legacy TLV sections (see legacy.go).
package sproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// HeaderSize is the fixed size of an encoded frame header in bytes.
const HeaderSize = 8

// HeaderMagic is the protocol magic that begins every frame header.
const HeaderMagic uint32 = 0x25521814

// HeaderVersion is the protocol version carried by every frame header. It
// occupies a single byte at offset 4.
const HeaderVersion uint8 = 0x46

// Command words carried in the header's cmd field. The handshake uses distinct
// commands; once the channel is encrypted, requests almost always use CmdAction
// and dispatch on the PObject "action" string instead.
const (
	// CmdAction is the post-handshake command. The actual operation is selected
	// by the "action" string inside the request PObject, not by this word.
	CmdAction uint8 = 0x01
	// CmdTestConn probes a server before the handshake. Its reply is the
	// legacy TLV frame decoded by DecodeTestConnReply (see legacy.go).
	CmdTestConn uint8 = 0x0b
	// CmdAuth carries the authentication request (mode C pairing / mode A reuse).
	CmdAuth uint8 = 0x12
	// CmdRequestSSL requests the channel encryption (encrypt_channel) that
	// precedes the in-place TLS upgrade.
	CmdRequestSSL uint8 = 0x16
)

// Sentinel errors returned when decoding a malformed header. Callers can match
// them with errors.Is.
var (
	// ErrBadMagic is returned when the magic field does not equal HeaderMagic.
	ErrBadMagic = errors.New("sproto: bad header magic")
	// ErrBadVersion is returned when the version field does not equal
	// HeaderVersion.
	ErrBadVersion = errors.New("sproto: unsupported header version")
)

// Header is the fixed 8-byte frame header that prefixes a client request.
type Header struct {
	// Cmd is the command byte. After the TLS upgrade it is almost always 0x01,
	// with dispatch happening on the PObject "action" string instead.
	Cmd uint8
	// PktLen is the payload-length hint at offset 6..7. It is a hint only - the
	// server sends 0 and bodies are self-delimiting - so it is preserved on
	// decode for diagnostics but never used for framing.
	PktLen uint16
}

// MarshalBinary encodes h into its 8-byte big-endian wire form.
func (h Header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], HeaderMagic)
	buf[4] = HeaderVersion
	buf[5] = h.Cmd
	binary.BigEndian.PutUint16(buf[6:8], h.PktLen)
	return buf, nil
}

// WriteTo encodes h and writes it to w. It implements io.WriterTo.
func (h Header) WriteTo(w io.Writer) (int64, error) {
	buf, _ := h.MarshalBinary()
	n, err := w.Write(buf)
	return int64(n), err
}

// UnmarshalBinary decodes an 8-byte header from buf, validating the magic and
// version fields.
func (h *Header) UnmarshalBinary(buf []byte) error {
	if len(buf) < HeaderSize {
		return fmt.Errorf("sproto: short header: have %d bytes, need %d", len(buf), HeaderSize)
	}
	if magic := binary.BigEndian.Uint32(buf[0:4]); magic != HeaderMagic {
		return fmt.Errorf("%w: 0x%08x", ErrBadMagic, magic)
	}
	if version := buf[4]; version != HeaderVersion {
		return fmt.Errorf("%w: 0x%02x", ErrBadVersion, version)
	}
	h.Cmd = buf[5]
	h.PktLen = binary.BigEndian.Uint16(buf[6:8])
	return nil
}

// ReadHeader reads exactly 8 bytes from r and decodes them into a Header,
// validating the magic and version fields.
func ReadHeader(r io.Reader) (Header, error) {
	var buf [HeaderSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, err
	}
	var h Header
	if err := h.UnmarshalBinary(buf[:]); err != nil {
		return Header{}, err
	}
	return h, nil
}
