package sproto

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	for _, cmd := range []uint8{0x00, 0x01, 0x0b, 0x12, 0x16, 0x46, 0xff} {
		for _, pktLen := range []uint16{0, 1, 0xffff} {
			var buf bytes.Buffer
			want := Header{Cmd: cmd, PktLen: pktLen}
			if _, err := want.WriteTo(&buf); err != nil {
				t.Fatalf("WriteTo(cmd=0x%02x): %v", cmd, err)
			}
			if buf.Len() != HeaderSize {
				t.Fatalf("encoded length = %d, want %d", buf.Len(), HeaderSize)
			}
			got, err := ReadHeader(&buf)
			if err != nil {
				t.Fatalf("ReadHeader(cmd=0x%02x): %v", cmd, err)
			}
			if got != want {
				t.Errorf("round-trip cmd=0x%02x: got %+v, want %+v", cmd, got, want)
			}
		}
	}
}

// TestHeaderWireLayout pins the byte-for-byte layout a live DSM accepts:
// version is ONE byte at offset 4 and cmd ONE byte at offset 5, followed by the
// uint16 pkt_len hint. The previous expectation - version as the uint16 0x0046
// and cmd as a uint16 - put 0x00 where the server reads the version, and every
// command was rejected with a canned error frame.
func TestHeaderWireLayout(t *testing.T) {
	h := Header{Cmd: CmdTestConn}
	buf, err := h.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	want := []byte{0x25, 0x52, 0x18, 0x14, 0x46, 0x0b, 0x00, 0x00}
	if !bytes.Equal(buf, want) {
		t.Errorf("wire layout = % x, want % x", buf, want)
	}
}

// TestHeaderPktLenLayout confirms pkt_len occupies bytes 6..7 big-endian.
func TestHeaderPktLenLayout(t *testing.T) {
	buf, err := Header{Cmd: CmdAction, PktLen: 0x0102}.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	want := []byte{0x25, 0x52, 0x18, 0x14, 0x46, 0x01, 0x01, 0x02}
	if !bytes.Equal(buf, want) {
		t.Errorf("wire layout = % x, want % x", buf, want)
	}
}

func TestReadHeaderBadMagic(t *testing.T) {
	buf := []byte{0xde, 0xad, 0xbe, 0xef, 0x46, 0x01, 0x00, 0x00}
	_, err := ReadHeader(bytes.NewReader(buf))
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("got err %v, want ErrBadMagic", err)
	}
}

func TestReadHeaderBadVersion(t *testing.T) {
	buf := []byte{0x25, 0x52, 0x18, 0x14, 0x99, 0x01, 0x00, 0x00}
	_, err := ReadHeader(bytes.NewReader(buf))
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("got err %v, want ErrBadVersion", err)
	}
}

// TestReadHeaderRejectsPreFixLayout pins the exact regression: the old encoder
// wrote version as the uint16 0x0046, leaving 0x00 at offset 4 - which a live
// DSM reads as an invalid version and rejects. Decoding such a frame must fail
// with ErrBadVersion rather than silently reading cmd out of the wrong byte.
func TestReadHeaderRejectsPreFixLayout(t *testing.T) {
	preFix := []byte{0x25, 0x52, 0x18, 0x14, 0x00, 0x46, 0x00, 0x0b}
	_, err := ReadHeader(bytes.NewReader(preFix))
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("got err %v, want ErrBadVersion", err)
	}
}

func TestReadHeaderShort(t *testing.T) {
	buf := []byte{0x25, 0x52, 0x18}
	_, err := ReadHeader(bytes.NewReader(buf))
	if err == nil {
		t.Fatal("expected error on short header, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got err %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestUnmarshalBinaryShort(t *testing.T) {
	var h Header
	if err := h.UnmarshalBinary([]byte{0x25, 0x52}); err == nil {
		t.Fatal("expected error on short buffer, got nil")
	}
}
