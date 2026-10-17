package sproto

import (
	"encoding/binary"
	"errors"
	"testing"
)

// buildLegacyFrame assembles a legacy TLV body from sections so tests can
// describe a reply at the byte level. RES and PROTO_VERSION are single-byte
// fixed fields; vardata fields carry a big-endian uint16 length prefix. This is
// the BODY only - the 8-byte frame header the server prefixes is stripped by the
// caller (transport.testConn) before decoding.
func buildLegacyFrame(t *testing.T) []byte {
	t.Helper()
	var b []byte
	// RES = 1 (the legacy frame reports success as 1, not 0)
	b = append(b, legacyTagRes, legacySuccess)
	// PROTO_VERSION = 0x46
	b = append(b, legacyTagProtoVersion, HeaderVersion)
	// SERIAL_NUMBER = 32-hex ds_id
	dsID := "0123456789abcdef0123456789abcdef"
	b = append(b, legacyTagSerialNumber)
	b = binary.BigEndian.AppendUint16(b, uint16(len(dsID)))
	b = append(b, dsID...)
	// SERVER_ID
	serverID := "synology-server"
	b = append(b, legacyTagServerID)
	b = binary.BigEndian.AppendUint16(b, uint16(len(serverID)))
	b = append(b, serverID...)
	return b
}

// TestDecodeTestConnReply_LiveGolden decodes the exact 44-byte body a live DSM
// 7.x / Synology Drive Server 4.0.3 returned to a TEST_CONN probe, captured off
// the wire. It is the fixture the whole class of framing bugs turned on: the
// hand-written mock had guessed different tags and value widths, so it agreed
// with the client's wrong assumptions and could never fail.
func TestDecodeTestConnReply_LiveGolden(t *testing.T) {
	// 25 52 18 14 46 0b 00 00 | 01 01 1c 46 1b 00 20 <32-byte ds_id> 1d 00 02 2d 2d
	body := []byte{
		0x01, 0x01, // RES = 1
		0x1c, 0x46, // PROTO_VERSION = 0x46
		0x1b, 0x00, 0x20, // SERIAL_NUMBER, len 32
		'8', 'b', '8', '9', '1', 'b', '2', 'f', '8', 'f', 'c', '0', '0', 'd', '5', 'a',
		'4', '5', '5', '7', '5', 'b', '5', '4', 'b', '3', '5', '1', '5', 'e', 'b', 'a',
		0x1d, 0x00, 0x02, '-', '-', // SERVER_ID = "--"
	}
	if len(body) != 44 {
		t.Fatalf("fixture is %d bytes, want the captured 44", len(body))
	}
	reply, err := DecodeTestConnReply(body)
	if err != nil {
		t.Fatalf("DecodeTestConnReply on the live capture: %v", err)
	}
	if !reply.Succeeded() {
		t.Errorf("Succeeded() = false, Result = %d, want success (%d)", reply.Result, legacySuccess)
	}
	if reply.ProtoVersion != int32(HeaderVersion) {
		t.Errorf("ProtoVersion = %d, want %d", reply.ProtoVersion, HeaderVersion)
	}
	if want := "8b891b2f8fc00d5a45575b54b3515eba"; reply.DsID != want {
		t.Errorf("DsID = %q, want %q", reply.DsID, want)
	}
	if reply.ServerID != "--" {
		t.Errorf("ServerID = %q, want %q", reply.ServerID, "--")
	}
}

func TestTestConnRequest(t *testing.T) {
	got := TestConnRequest()
	if len(got) != HeaderSize {
		t.Fatalf("TestConnRequest length = %d, want %d", len(got), HeaderSize)
	}
	var h Header
	if err := h.UnmarshalBinary(got); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if h.Cmd != CmdTestConn {
		t.Fatalf("cmd = 0x%02x, want CmdTestConn 0x%02x", h.Cmd, CmdTestConn)
	}
}

func TestDecodeTestConnReply_Golden(t *testing.T) {
	frame := buildLegacyFrame(t)
	reply, err := DecodeTestConnReply(frame)
	if err != nil {
		t.Fatalf("DecodeTestConnReply: %v", err)
	}
	if !reply.Succeeded() {
		t.Errorf("Succeeded() = false, Result = %d, want success (%d)", reply.Result, legacySuccess)
	}
	if reply.ProtoVersion != 70 {
		t.Errorf("ProtoVersion = %d, want 70", reply.ProtoVersion)
	}
	want := "0123456789abcdef0123456789abcdef"
	if reply.DsID != want {
		t.Errorf("DsID = %q, want %q", reply.DsID, want)
	}
	if len(reply.DsID) != 32 {
		t.Errorf("DsID length = %d, want 32", len(reply.DsID))
	}
	if reply.ServerID != "synology-server" {
		t.Errorf("ServerID = %q, want %q", reply.ServerID, "synology-server")
	}
}

func TestDecodeTestConnReply_Empty(t *testing.T) {
	reply, err := DecodeTestConnReply(nil)
	if err != nil {
		t.Fatalf("DecodeTestConnReply(nil): %v", err)
	}
	if reply != (TestConnReply{}) {
		t.Errorf("got %+v, want zero value", reply)
	}
}

func TestDecodeTestConnReply_Errors(t *testing.T) {
	good := buildLegacyFrame(t)
	tests := []struct {
		name    string
		frame   []byte
		wantErr error // nil means "any error"
	}{
		{
			name:  "unknown tag",
			frame: []byte{0x7f, 0x00},
		},
		{
			name:    "truncated fixed field",
			frame:   []byte{legacyTagRes}, // RES is one byte wide; its value is missing
			wantErr: ErrLegacyTruncated,
		},
		{
			name:    "truncated vardata length prefix",
			frame:   []byte{legacyTagSerialNumber, 0x00}, // only 1 of 2 len bytes
			wantErr: ErrLegacyTruncated,
		},
		{
			name:    "truncated vardata body",
			frame:   []byte{legacyTagSerialNumber, 0x00, 0x08, 'a', 'b'}, // claims 8, has 2
			wantErr: ErrLegacyTruncated,
		},
		{
			name:  "oversized vardata",
			frame: append([]byte{legacyTagSerialNumber}, binary.BigEndian.AppendUint16(nil, maxLegacyVardata+1)...),
		},
		{
			name:  "truncated mid-frame after valid sections",
			frame: append(good[:len(good)-1], good[len(good)-1:len(good)-1]...), // drop last byte
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeTestConnReply(tt.frame)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeTestConnReply_SectionCap(t *testing.T) {
	// Many empty SERVER_ID sections (tag + 0x0000 length) exceed the section cap.
	var frame []byte
	for i := 0; i < maxLegacySections+1; i++ {
		frame = append(frame, legacyTagServerID, 0x00, 0x00)
	}
	if _, err := DecodeTestConnReply(frame); err == nil {
		t.Fatalf("expected section-cap error, got nil")
	}
}

func TestDecodeTestConnReply_SectionCapBoundary(t *testing.T) {
	// maxLegacySections is the number of sections a reply MAY carry, so an
	// exactly-cap frame must decode cleanly (the cap-check runs only once another
	// section actually remains). Regression pin for the off-by-one that rejected a
	// full-cap frame because the count was checked before the EOF probe.
	var frame []byte
	for i := 0; i < maxLegacySections; i++ {
		frame = append(frame, legacyTagServerID, 0x00, 0x00)
	}
	if _, err := DecodeTestConnReply(frame); err != nil {
		t.Fatalf("exactly %d sections should decode, got error: %v", maxLegacySections, err)
	}
	// One more section is the (cap+1)-th and must be rejected.
	frame = append(frame, legacyTagServerID, 0x00, 0x00)
	if _, err := DecodeTestConnReply(frame); err == nil {
		t.Fatalf("expected section-cap error at %d sections, got nil", maxLegacySections+1)
	}
}
