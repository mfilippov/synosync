package sproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

// encodeDecode round-trips v through the codec and returns the decoded value.
func encodeDecode(t *testing.T, v Value) Value {
	t.Helper()
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := NewDecoder(&buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("decoder left %d trailing bytes", buf.Len())
	}
	return got
}

// valuesEqual compares two decoded values structurally, ignoring Binary.Reader.
func valuesEqual(a, b Value) bool {
	switch av := a.(type) {
	case Null:
		_, ok := b.(Null)
		return ok
	case Int:
		bv, ok := b.(Int)
		return ok && av == bv
	case String:
		bv, ok := b.(String)
		return ok && av == bv
	case Binary:
		bv, ok := b.(Binary)
		return ok && av.Length == bv.Length && av.SendHash == bv.SendHash &&
			bytes.Equal(av.Data, bv.Data)
	case Array:
		bv, ok := b.(Array)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !valuesEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case *Map:
		bv, ok := b.(*Map)
		if !ok || !reflect.DeepEqual(av.keys, bv.keys) {
			return false
		}
		for _, k := range av.keys {
			x, _ := av.Get(k)
			y, _ := bv.Get(k)
			if !valuesEqual(x, y) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func TestRoundTripScalars(t *testing.T) {
	cases := []Value{
		Null{},
		Int(0),
		Int(1),
		Int(255),
		Int(256),
		Int(65535),
		Int(65536),
		Int(4294967295),
		Int(4294967296),
		Int(-1),
		Int(-123456789),
		String(""),
		String("hello"),
		String("ключ"),
	}
	for _, want := range cases {
		got := encodeDecode(t, want)
		if !valuesEqual(want, got) {
			t.Errorf("round-trip %#v: got %#v", want, got)
		}
	}
}

func TestIntMinSizeEncoding(t *testing.T) {
	cases := []struct {
		v    int64
		size int
	}{
		{0, 1}, {255, 1}, {256, 2}, {65535, 2}, {65536, 4},
		{4294967295, 4}, {4294967296, 8}, {-1, 8},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		if err := NewEncoder(&buf).Encode(Int(c.v)); err != nil {
			t.Fatalf("Encode(%d): %v", c.v, err)
		}
		// [tag][size][value...]: total length is 2 + size.
		if got := buf.Len() - 2; got != c.size {
			t.Errorf("Int(%d) encoded with %d value bytes, want %d", c.v, got, c.size)
		}
		if b := buf.Bytes(); b[0] != tagInt || int(b[1]) != c.size {
			t.Errorf("Int(%d) header = % x, want tag 0x01 size %d", c.v, b[:2], c.size)
		}
	}
}

func TestNullWireLayout(t *testing.T) {
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(Null{}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if want := []byte{0x00, 0x00}; !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("Null wire = % x, want % x", buf.Bytes(), want)
	}
}

func TestStringWireLayout(t *testing.T) {
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(String("hi")); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	want := []byte{tagString, 0x00, 0x02, 'h', 'i'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("String wire = % x, want % x", buf.Bytes(), want)
	}
}

func TestRoundTripNested(t *testing.T) {
	inner := NewMap().
		Set("name", String("file.txt")).
		Set("size", Int(4096)).
		Set("dir", Null{})
	want := NewMap().
		Set("action", String("list")).
		Set("items", Array{String("a"), Int(2), inner}).
		Set("nested", Array{Array{Int(1), Int(2)}, Array{}})
	got := encodeDecode(t, want)
	if !valuesEqual(want, got) {
		t.Errorf("nested round-trip mismatch:\n want %#v\n got  %#v", want, got)
	}
}

func TestUnderscoreKeyStripped(t *testing.T) {
	v := NewMap().Set("_action", String("list")).Set("path", String("/"))
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// The leading underscore must not appear on the wire...
	if bytes.Contains(buf.Bytes(), []byte("_action")) {
		t.Error("wire contains the stripped key \"_action\"")
	}
	if !bytes.Contains(buf.Bytes(), []byte("action")) {
		t.Error("wire is missing the stripped key \"action\"")
	}
	// ...and the decoder therefore sees "action".
	got, err := NewDecoder(&buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m, ok := got.(*Map)
	if !ok {
		t.Fatalf("decoded type %T, want *Map", got)
	}
	if _, ok := m.Get("action"); !ok {
		t.Error("decoded map missing key \"action\"")
	}
	if _, ok := m.Get("_action"); ok {
		t.Error("decoded map unexpectedly has key \"_action\"")
	}
}

// TestMapIntString exercises the typed Map.Int/Map.String accessors: a present
// well-typed value, a missing key, and a present-but-wrong-typed value.
func TestMapIntString(t *testing.T) {
	m := NewMap().
		Set("size", Int(42)).
		Set("name", String("hello")).
		Set("nullish", Null{})

	if v, ok := m.Int("size"); !ok || v != 42 {
		t.Errorf("Int(size) = (%d, %v), want (42, true)", v, ok)
	}
	if s, ok := m.String("name"); !ok || s != "hello" {
		t.Errorf("String(name) = (%q, %v), want (\"hello\", true)", s, ok)
	}

	// Missing keys report the zero value and false.
	if v, ok := m.Int("absent"); ok || v != 0 {
		t.Errorf("Int(absent) = (%d, %v), want (0, false)", v, ok)
	}
	if s, ok := m.String("absent"); ok || s != "" {
		t.Errorf("String(absent) = (%q, %v), want (\"\", false)", s, ok)
	}

	// Present-but-wrong-typed values report the zero value and false.
	if v, ok := m.Int("name"); ok || v != 0 {
		t.Errorf("Int(name) = (%d, %v), want (0, false)", v, ok)
	}
	if s, ok := m.String("size"); ok || s != "" {
		t.Errorf("String(size) = (%q, %v), want (\"\", false)", s, ok)
	}
	if v, ok := m.Int("nullish"); ok || v != 0 {
		t.Errorf("Int(nullish) = (%d, %v), want (0, false)", v, ok)
	}
}

func TestProtoEnvelopePreserved(t *testing.T) {
	proto := NewMap().
		Set("type", String("header")).
		Set("body-continue", Int(0))
	v := NewMap().Set("@proto", proto).Set("action", String("query_user_info"))
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(v); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// The "@" prefix is NOT stripped.
	if !bytes.Contains(buf.Bytes(), []byte("@proto")) {
		t.Error("wire is missing the \"@proto\" envelope key")
	}
	got := encodeDecode(t, v)
	if !valuesEqual(v, got) {
		t.Errorf("@proto round-trip mismatch:\n want %#v\n got  %#v", v, got)
	}
}

func TestBareVsHeaderedFraming(t *testing.T) {
	body := NewMap().Set("action", String("list"))

	// Headered: client request carries the 8-byte header.
	var req bytes.Buffer
	if err := WriteRequest(&req, 0x01, body); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	h, err := ReadHeader(&req)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Cmd != 0x01 {
		t.Errorf("Cmd = 0x%02x, want 0x01", h.Cmd)
	}
	gotReq, err := NewDecoder(&req).Decode()
	if err != nil {
		t.Fatalf("Decode request body: %v", err)
	}
	if !valuesEqual(body, gotReq) {
		t.Error("headered request body mismatch")
	}

	// Bare: server response is a PObject with no header.
	var resp bytes.Buffer
	if err := NewEncoder(&resp).Encode(body); err != nil {
		t.Fatalf("Encode response: %v", err)
	}
	gotResp, err := ReadResponse(&resp)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if !valuesEqual(body, gotResp) {
		t.Error("bare response body mismatch")
	}
}

func TestBinaryRoundTrip(t *testing.T) {
	payload := []byte("the quick brown fox")
	got := encodeDecode(t, Binary{Data: payload})
	b, ok := got.(Binary)
	if !ok {
		t.Fatalf("decoded type %T, want Binary", got)
	}
	if b.Length != int64(len(payload)) || !bytes.Equal(b.Data, payload) {
		t.Errorf("binary round-trip: len=%d data=%q", b.Length, b.Data)
	}
}

func TestBinaryFromReader(t *testing.T) {
	payload := []byte("streamed-from-a-reader")
	v := Binary{Length: int64(len(payload)), Reader: bytes.NewReader(payload)}
	got := encodeDecode(t, v)
	b := got.(Binary)
	if !bytes.Equal(b.Data, payload) {
		t.Errorf("reader-sourced binary = %q, want %q", b.Data, payload)
	}
}

func TestBinaryExRoundTrip(t *testing.T) {
	payload := []byte("hash-checked content")
	want := Binary{Data: payload, SendHash: "0123456789abcdef"}
	got := encodeDecode(t, want)
	b, ok := got.(Binary)
	if !ok {
		t.Fatalf("decoded type %T, want Binary", got)
	}
	if b.SendHash != want.SendHash || !bytes.Equal(b.Data, payload) {
		t.Errorf("binaryex round-trip: hash=%q data=%q", b.SendHash, b.Data)
	}
	// The 0x43 framing must actually be used.
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(want); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if buf.Bytes()[0] != tagBinaryEx {
		t.Errorf("first byte = 0x%02x, want 0x43 (BinaryEx)", buf.Bytes()[0])
	}
}

func TestBinaryStreamingNotBuffered(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 4096)
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(Binary{Data: payload}); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var streamed bytes.Buffer
	var handled Binary
	dec := NewDecoder(&buf)
	dec.MaxInlineBinary = 64 // smaller than the payload
	dec.BinaryStream = func(b Binary, r io.Reader) error {
		handled = b
		_, err := io.Copy(&streamed, r)
		return err
	}
	got, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	b := got.(Binary)
	if b.Data != nil {
		t.Error("oversized binary was buffered into Data, want nil")
	}
	if b.Length != int64(len(payload)) || handled.Length != int64(len(payload)) {
		t.Errorf("Length = %d / handled %d, want %d", b.Length, handled.Length, len(payload))
	}
	if !bytes.Equal(streamed.Bytes(), payload) {
		t.Error("streamed payload differs from the original")
	}
	if buf.Len() != 0 {
		t.Errorf("decoder left %d trailing bytes after streaming", buf.Len())
	}
}

func TestStreamingDrainsWhenHandlerSkips(t *testing.T) {
	// A handler that reads nothing must still leave the stream aligned for the
	// following value.
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	if err := enc.Encode(Array{
		Binary{Data: bytes.Repeat([]byte{0x01}, 1000)},
		String("after"),
	}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dec := NewDecoder(&buf)
	dec.MaxInlineBinary = 16
	dec.BinaryStream = func(Binary, io.Reader) error { return nil } // reads nothing
	got, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	arr := got.(Array)
	if s, ok := arr[1].(String); !ok || s != "after" {
		t.Errorf("trailing value = %#v, want String(\"after\")", arr[1])
	}
}

func TestDecodeErrors(t *testing.T) {
	t.Run("unknown tag", func(t *testing.T) {
		_, err := NewDecoder(bytes.NewReader([]byte{0x99})).Decode()
		if err == nil {
			t.Fatal("expected error on unknown tag")
		}
	})
	t.Run("top-level end", func(t *testing.T) {
		_, err := NewDecoder(bytes.NewReader([]byte{tagEnd})).Decode()
		if !errors.Is(err, ErrUnexpectedEnd) {
			t.Fatalf("got %v, want ErrUnexpectedEnd", err)
		}
	})
	t.Run("non-string map key", func(t *testing.T) {
		// Map containing an Int where a string key is required.
		_, err := NewDecoder(bytes.NewReader([]byte{tagMap, tagInt, 0x01, 0x05})).Decode()
		if err == nil {
			t.Fatal("expected error on non-string map key")
		}
	})
	t.Run("bad int size", func(t *testing.T) {
		_, err := NewDecoder(bytes.NewReader([]byte{tagInt, 0x03, 0, 0, 0})).Decode()
		if err == nil {
			t.Fatal("expected error on invalid int size")
		}
	})
	t.Run("truncated string", func(t *testing.T) {
		_, err := NewDecoder(bytes.NewReader([]byte{tagString, 0x00, 0x05, 'a'})).Decode()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("null with non-zero trailer", func(t *testing.T) {
		// A Null record must be followed by 0x00; a non-zero trailer is a
		// malformed frame that would otherwise desync nested decoding.
		_, err := NewDecoder(bytes.NewReader([]byte{tagNull, 0xff})).Decode()
		if err == nil {
			t.Fatal("expected error on non-zero null trailer")
		}
	})
	t.Run("truncated streamed binary", func(t *testing.T) {
		// An oversized binary whose payload ends early must surface as
		// truncation rather than a clean decode, even if the handler reads
		// everything it is given.
		frame := []byte{tagBinary, 0, 0, 0, 0, 0, 0, 0, 0x40} // declares 64 bytes
		frame = append(frame, bytes.Repeat([]byte{0x01}, 8)...)
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxInlineBinary = 16 // route through streamBinary
		dec.BinaryStream = func(_ Binary, r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			return err
		}
		_, err := dec.Decode()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("forged binary length does not pre-allocate", func(t *testing.T) {
		// A large (but within-budget) length followed by only a couple of bytes
		// must error cleanly via incremental copy rather than pre-allocating the
		// claimed buffer. 1 MiB stays under the default frame budget so the
		// truncation path - not the aggregate guard - is what rejects it.
		frame := []byte{tagBinary, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 'a', 'b'}
		_, err := NewDecoder(bytes.NewReader(frame)).Decode()
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("forged exabyte length rejected before allocation", func(t *testing.T) {
		// On a bare Decoder the default inline cap routes a near-MaxInt64 claim
		// onto the drained streaming path, so it never reaches the buffering
		// allocation at all. The drain budget then refuses the claim outright,
		// before a single payload byte is read - previously
		// the drain ran to the claimed end and this frame surfaced as a
		// truncation only because the reader happened to be short. Within-budget
		// forged lengths still take the truncation path (see the subtest above);
		// the aggregate frame-budget rejection of the same claim (with the inline
		// cap explicitly opted out) is pinned by TestFrameBudgetGiantClaimedLength.
		frame := []byte{tagBinary, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 'a', 'b'}
		_, err := NewDecoder(bytes.NewReader(frame)).Decode()
		if !errors.Is(err, ErrDrainTooLarge) {
			t.Fatalf("got %v, want ErrDrainTooLarge", err)
		}
	})
	t.Run("excessive nesting depth", func(t *testing.T) {
		// A long run of array-open tags must be rejected, not followed until the
		// goroutine stack overflows.
		frame := make([]byte, maxDepth+2)
		for i := range frame {
			frame[i] = tagArray
		}
		_, err := NewDecoder(bytes.NewReader(frame)).Decode()
		if err == nil {
			t.Fatal("expected error on excessive nesting depth")
		}
	})
	t.Run("depth reset on reused decoder", func(t *testing.T) {
		// Decode resets depth (alongside nodes/frameBytes), so a stale positive
		// depth left on a reused decoder does not shrink the next frame's nesting
		// budget. Simulate the stale state directly and confirm a valid frame that
		// approaches (but stays within) the depth limit still decodes.
		frame := make([]byte, maxDepth)
		for i := range frame {
			frame[i] = tagArray
		}
		// Each array is unterminated; the reader hitting EOF after the opens is the
		// expected outcome - what matters is that we reach EOF rather than a
		// premature "nesting depth exceeds" error from a leaked depth.
		dec := NewDecoder(bytes.NewReader(frame))
		dec.depth = maxDepth // pretend a prior path leaked a maxed-out depth
		_, err := dec.Decode()
		if err == nil {
			t.Fatal("expected an error (unterminated arrays hit EOF)")
		}
		if strings.Contains(err.Error(), "nesting depth exceeds") {
			t.Fatalf("depth was not reset: %v", err)
		}
	})
	t.Run("excessive value count", func(t *testing.T) {
		// A flat array of tiny tags must be bounded by node count, not followed
		// until the slice growth exhausts memory. Each Null element is two bytes
		// (tag + trailing zero); maxNodes+1 of them exceeds the budget. The frame
		// budget is raised past what the per-node floor would charge for that many
		// nodes so the node COUNTER is what trips here - it is the backstop for a
		// caller that widens MaxFrameBytes (see nodeCostBytes).
		var frame bytes.Buffer
		frame.WriteByte(tagArray)
		for i := 0; i <= maxNodes; i++ {
			frame.WriteByte(tagNull)
			frame.WriteByte(0x00)
		}
		dec := NewDecoder(bytes.NewReader(frame.Bytes()))
		dec.MaxFrameBytes = (maxNodes + 2) * nodeCostBytes
		_, err := dec.Decode()
		if err == nil {
			t.Fatal("expected error on excessive value count")
		}
		if !strings.Contains(err.Error(), "value count exceeds") {
			t.Fatalf("got %v, want the node-count error", err)
		}
	})
}

func TestFrameByteBudget(t *testing.T) {
	// A flat array of many sub-cap Binary values whose payloads sum past the
	// frame budget must be rejected with ErrFrameTooLarge rather than buffered
	// whole. Each element is below any single-value cap, so only the aggregate
	// guard catches it.
	const each = 256
	const count = 20
	var buf bytes.Buffer
	elems := make(Array, count)
	for i := range elems {
		elems[i] = Binary{Data: bytes.Repeat([]byte{0xCD}, each)}
	}
	if err := NewEncoder(&buf).Encode(elems); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	frame := buf.Bytes()

	t.Run("over budget errors", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxFrameBytes = each * count / 2 // half the aggregate payload
		_, err := dec.Decode()
		if !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("within budget succeeds", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxFrameBytes = each*count + 1024 // headroom for the whole payload
		got, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if arr, ok := got.(Array); !ok || len(arr) != count {
			t.Fatalf("got %T len %d, want Array len %d", got, len(got.(Array)), count)
		}
	})

	t.Run("string bodies count too", func(t *testing.T) {
		var sbuf bytes.Buffer
		sa := make(Array, count)
		for i := range sa {
			sa[i] = String(bytes.Repeat([]byte{'x'}, each))
		}
		if err := NewEncoder(&sbuf).Encode(sa); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		dec := NewDecoder(&sbuf)
		dec.MaxFrameBytes = each * count / 2
		if _, err := dec.Decode(); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("streamed binaries are not charged", func(t *testing.T) {
		// A single oversized Binary is streamed (drained, not buffered) so it must
		// not consume the frame budget even when its length dwarfs it.
		var bbuf bytes.Buffer
		if err := NewEncoder(&bbuf).Encode(Binary{Data: bytes.Repeat([]byte{0x01}, each*count)}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		dec := NewDecoder(&bbuf)
		dec.MaxInlineBinary = 32           // route through streamBinary
		dec.MaxFrameBytes = each*count - 1 // smaller than the payload
		dec.BinaryStream = func(_ Binary, r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			return err
		}
		if _, err := dec.Decode(); err != nil {
			t.Fatalf("Decode: %v, want nil (streamed payload must not count)", err)
		}
	})
}

// countingReader tallies the bytes actually pulled from a frame, so a test can
// prove the decoder bailed out partway instead of materialising the whole thing.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// TestFrameBudgetChargesNodeCost is the node-cost guard: a frame whose
// PAYLOAD is empty but whose node count is enormous costs a fortune in heap
// (~96 bytes per empty *Map, plus as much again when actions re-materialises the
// page with make(..., len(arr))) while its wire size sits far below the budget.
// Charging payload bytes only, such a ~2 MiB frame decoded happily into ~100 MB;
// the per-node floor must reject it - before the allocation, partway through the
// frame.
func TestFrameBudgetChargesNodeCost(t *testing.T) {
	var frame bytes.Buffer
	frame.WriteByte(tagArray)
	// One under maxNodes counting the enclosing array, so the node COUNTER is not
	// what rejects this: before the floor existed the whole frame decoded.
	for i := 0; i < maxNodes-1; i++ {
		frame.WriteByte(tagMap) // an empty map: open + end, two bytes
		frame.WriteByte(tagEnd)
	}
	frame.WriteByte(tagEnd)
	if wire := int64(frame.Len()); wire >= defaultMaxFrameBytes {
		t.Fatalf("frame is %d wire bytes; the point is that it stays under the %d-byte budget", wire, defaultMaxFrameBytes)
	}
	cr := &countingReader{r: bytes.NewReader(frame.Bytes())}
	if _, err := NewDecoder(cr).Decode(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
	// The budget affords defaultMaxFrameBytes/nodeCostBytes nodes - half of what
	// this frame carries - so a decoder that honours it stops mid-frame.
	if cr.n >= int64(frame.Len()) {
		t.Fatalf("decoder consumed %d of %d bytes; want a bail-out before the end", cr.n, frame.Len())
	}
}

// TestFrameBudgetNodeCostBoundary pins the per-value accounting rule: a value
// costs max(payload bytes, nodeCostBytes) of the budget - the floor and the
// payload are not summed - and map keys, which are not values, pay their bytes
// in full on top of their value's floor. Each case decodes at its exact cost and
// fails one byte below it.
func TestFrameBudgetNodeCostBoundary(t *testing.T) {
	long := strings.Repeat("x", 3*nodeCostBytes)
	cases := []struct {
		name string
		v    Value
		want int64
	}{
		{"empty map is the floor", NewMap(), nodeCostBytes},
		{"null is the floor", Null{}, nodeCostBytes},
		{"int is the floor", Int(1 << 40), nodeCostBytes},
		{"short string is the floor, not floor+payload", String("hi"), nodeCostBytes},
		{"long string costs its payload, not payload+floor", String(long), int64(len(long))},
		{"inline binary costs its payload", Binary{Data: bytes.Repeat([]byte{7}, 1000)}, 1000},
		{"array charges every element", Array{Null{}, Null{}, Null{}}, 4 * nodeCostBytes},
		{"map charges key bytes on top of the value floor", NewMap().Set("ab", Null{}), 2*nodeCostBytes + 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := NewEncoder(&buf).Encode(tc.v); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			frame := buf.Bytes()

			dec := NewDecoder(bytes.NewReader(frame))
			dec.MaxFrameBytes = tc.want
			if _, err := dec.Decode(); err != nil {
				t.Fatalf("Decode at budget %d: %v, want success", tc.want, err)
			}
			if dec.frameBytes != tc.want {
				t.Errorf("charged %d bytes, want %d", dec.frameBytes, tc.want)
			}

			tight := NewDecoder(bytes.NewReader(frame))
			tight.MaxFrameBytes = tc.want - 1
			if _, err := tight.Decode(); !errors.Is(err, ErrFrameTooLarge) {
				t.Fatalf("Decode at budget %d: %v, want ErrFrameTooLarge", tc.want-1, err)
			}
		})
	}
}

// TestFrameBudgetRealisticPagesFit is the other half of the node-cost contract: the
// node floor must be small enough that genuine replies - a pull_event page and a
// large directory listing - still decode under the default budget with room to
// spare. Both fixtures are far larger than a live DSM pages.
func TestFrameBudgetRealisticPagesFit(t *testing.T) {
	// A catalog/event map as the server sends it: ~10 fields of realistic width.
	entry := func(i int) *Map {
		return NewMap().
			Set("sync_id", Int(int64(1_000_000+i))).
			Set("action", String("create")).
			Set("file_id", String(strings.Repeat("f", 36))).
			Set("path", String("team/projects/2026/"+strings.Repeat("n", 40)+"/file.bin")).
			Set("is_dir", Int(0)).
			Set("size", Int(1<<28)).
			Set("hash", String(strings.Repeat("a", 32))).
			Set("mtime", Int(1_700_000_000)).
			Set("committer_session_id", String(strings.Repeat("s", 32))).
			Set("from_path", String(""))
	}
	cases := []struct {
		name  string
		key   string
		count int
	}{
		{"pull_event page", "event_list", 5_000},
		{"large directory listing", "file_list", 20_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr := make(Array, tc.count)
			for i := range arr {
				arr[i] = entry(i)
			}
			var buf bytes.Buffer
			if err := NewEncoder(&buf).Encode(NewMap().Set("errno", Int(0)).Set(tc.key, arr)); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			dec := NewDecoder(&buf)
			got, err := dec.Decode()
			if err != nil {
				t.Fatalf("Decode: %v, want success under the default budget", err)
			}
			m, ok := got.(*Map)
			if !ok {
				t.Fatalf("decoded %T, want *Map", got)
			}
			list, _ := m.Get(tc.key)
			if decoded, ok := list.(Array); !ok || len(decoded) != tc.count {
				t.Fatalf("decoded list is %T len %d, want Array len %d", list, len(decoded), tc.count)
			}
			// Headroom, not a squeeze: even these deliberately fat fixtures must sit
			// inside half the budget, i.e. the floor tolerates twice as much again
			// (~1.5 KiB per ten-field entry ⇒ ~45k entries in ONE reply) before it
			// rejects anything. Live DSM pages are orders of magnitude smaller.
			if dec.frameBytes > defaultMaxFrameBytes/2 {
				t.Errorf("charged %d bytes, over half the %d-byte budget: nodeCostBytes is too coarse",
					dec.frameBytes, int64(defaultMaxFrameBytes))
			}
		})
	}
}

func TestEncodeNilValue(t *testing.T) {
	if err := NewEncoder(io.Discard).Encode(nil); err == nil {
		t.Fatal("expected error encoding nil value")
	}
}

// TestEncodeTypedNilMap pins the typed-nil guard: a nil *Map is a
// NON-nil Value interface, so it misses the nil case and used to panic dereferencing the
// map's key slice - a request-builder slip anywhere above the codec would have taken the
// whole daemon down instead of failing one turn. Every shape a nil *Map can arrive in
// must report an error and leave nothing half-written.
func TestEncodeTypedNilMap(t *testing.T) {
	var nilMap *Map
	cases := []struct {
		name string
		v    Value
	}{
		{"top level", nilMap},
		{"array element", Array{Int(1), nilMap}},
		{"map value", NewMap().Set("k", nilMap)},
		{"nested map value", NewMap().Set("outer", NewMap().Set("inner", nilMap))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := NewEncoder(&buf).Encode(c.v)
			if err == nil {
				t.Fatalf("Encode(%s) = nil error, want a typed-nil rejection", c.name)
			}
			if !strings.Contains(err.Error(), "nil") {
				t.Errorf("Encode(%s) error = %v, want it to name the nil value", c.name, err)
			}
		})
	}
}

func TestStringTooLong(t *testing.T) {
	big := String(make([]byte, 0x10000))
	if err := NewEncoder(io.Discard).Encode(big); err == nil {
		t.Fatal("expected error encoding over-long string")
	}
}

// randomValue builds a random value tree up to the given depth for the
// round-trip property test.
func randomValue(rng *rand.Rand, depth int) Value {
	if depth <= 0 {
		switch rng.Intn(4) {
		case 0:
			return Null{}
		case 1:
			return Int(rng.Int63() - rng.Int63())
		case 2:
			return String(randomString(rng))
		default:
			b := randomBytes(rng)
			return Binary{Length: int64(len(b)), Data: b}
		}
	}
	switch rng.Intn(6) {
	case 0:
		return Null{}
	case 1:
		return Int(rng.Int63() - rng.Int63())
	case 2:
		return String(randomString(rng))
	case 3:
		n := rng.Intn(4)
		arr := make(Array, n)
		for i := range arr {
			arr[i] = randomValue(rng, depth-1)
		}
		return arr
	default:
		m := NewMap()
		for i := 0; i < rng.Intn(4); i++ {
			// Keys without a leading underscore so the strip rule does not alter
			// them (the rule is exercised separately).
			m.Set("k"+randomString(rng), randomValue(rng, depth-1))
		}
		return m
	}
}

func randomString(rng *rand.Rand) string {
	b := make([]byte, rng.Intn(8))
	for i := range b {
		b[i] = byte('a' + rng.Intn(26))
	}
	return string(b)
}

func randomBytes(rng *rand.Rand) []byte {
	b := make([]byte, rng.Intn(16))
	for i := range b {
		b[i] = byte(rng.Intn(256))
	}
	return b
}

// giantBinaryRecord hand-crafts a 0x30 record claiming a MaxInt64-byte payload
// with no bytes behind it - the Encoder cannot produce a length its payload
// does not back, so hostile frames are built raw.
func giantBinaryRecord() []byte {
	rec := []byte{tagBinary}
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(math.MaxInt64))
	return append(rec, lenBuf[:]...)
}

// TestFrameBudgetGiantClaimedLength verifies the frame-byte guard rejects a
// forged Binary length near MaxInt64 before any allocation, in both positions:
// as the first charged value, and - the wrap case - after earlier charged
// bytes, where an add-then-check guard would overflow the running total
// negative and slip the claim onto the buffering path. MaxInlineBinary is set
// to -1 (explicit opt-out) so the claim reaches the buffering path at all;
// under the default inline cap it would be streamed and drained instead.
func TestFrameBudgetGiantClaimedLength(t *testing.T) {
	t.Run("single value", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(giantBinaryRecord()))
		dec.MaxInlineBinary = -1
		if _, err := dec.Decode(); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})

	t.Run("after charged bytes", func(t *testing.T) {
		// Array of [String "x", Binary claiming MaxInt64]: the string charges one
		// byte first, so frameBytes+n wraps negative under add-then-check.
		frame := []byte{tagArray, tagString, 0x00, 0x01, 'x'}
		frame = append(frame, giantBinaryRecord()...)
		frame = append(frame, tagEnd)
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxInlineBinary = -1
		if _, err := dec.Decode(); !errors.Is(err, ErrFrameTooLarge) {
			t.Fatalf("got %v, want ErrFrameTooLarge", err)
		}
	})
}

// TestMaxInlineBinaryDefault pins the zero-value semantics of MaxInlineBinary:
// zero selects the 1 MiB default (hostile-input-safe by construction for a
// bare Decoder), a value at the cap still inlines, and a negative value is the
// explicit opt-out that restores unlimited inlining.
func TestMaxInlineBinaryDefault(t *testing.T) {
	over := bytes.Repeat([]byte{0xEE}, defaultMaxInlineBinary+1)
	var overFrame bytes.Buffer
	if err := NewEncoder(&overFrame).Encode(Binary{Data: over}); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	t.Run("zero cap streams past the default", func(t *testing.T) {
		got, err := NewDecoder(bytes.NewReader(overFrame.Bytes())).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		b, ok := got.(Binary)
		if !ok {
			t.Fatalf("got %T, want Binary", got)
		}
		if b.Data != nil {
			t.Fatalf("Data inlined (%d bytes), want nil under the default cap", len(b.Data))
		}
		if b.Length != int64(len(over)) {
			t.Fatalf("Length = %d, want %d", b.Length, len(over))
		}
	})

	t.Run("negative cap opts out", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(overFrame.Bytes()))
		dec.MaxInlineBinary = -1
		got, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		b, ok := got.(Binary)
		if !ok {
			t.Fatalf("got %T, want Binary", got)
		}
		if !bytes.Equal(b.Data, over) {
			t.Fatalf("Data = %d bytes, want the full %d-byte payload inlined", len(b.Data), len(over))
		}
	})

	t.Run("at the default cap still inlines", func(t *testing.T) {
		exact := over[:defaultMaxInlineBinary]
		var frame bytes.Buffer
		if err := NewEncoder(&frame).Encode(Binary{Data: exact}); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := NewDecoder(&frame).Decode()
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		b, ok := got.(Binary)
		if !ok {
			t.Fatalf("got %T, want Binary", got)
		}
		if !bytes.Equal(b.Data, exact) {
			t.Fatalf("Data = %d bytes, want the full %d-byte payload inlined", len(b.Data), len(exact))
		}
	})
}

func TestRoundTripProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		want := randomValue(rng, 4)
		var buf bytes.Buffer
		if err := NewEncoder(&buf).Encode(want); err != nil {
			t.Fatalf("iter %d Encode: %v", i, err)
		}
		got, err := NewDecoder(&buf).Decode()
		if err != nil {
			t.Fatalf("iter %d Decode: %v", i, err)
		}
		if buf.Len() != 0 {
			t.Fatalf("iter %d: %d trailing bytes", i, buf.Len())
		}
		if !valuesEqual(want, got) {
			t.Fatalf("iter %d mismatch:\n want %#v\n got  %#v", i, want, got)
		}
	}
}

// countingWriter records every Write call made on the underlying writer so the
// tests can pin how the encoder's frame buffering coalesces (or deliberately
// bypasses, for streamed payloads) the wire writes.
type countingWriter struct {
	buf   bytes.Buffer
	sizes []int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return w.buf.Write(p)
}

// typicalActionRequest mirrors the shape of a real action-client metadata
// request: an @proto envelope plus a handful of scalar params. Unbuffered, its
// encoding cost ~30 Write calls (each tag, length prefix and body a separate
// write).
func typicalActionRequest() *Map {
	return NewMap().
		Set("@proto", NewMap().Set("type", String("request")).Set("version", Int(1))).
		Set("action", String("resume_upload")).
		Set("session", String("0123456789abcdef")).
		Set("view_id", Int(42)).
		Set("path", String("dir/subdir/file.bin")).
		Set("body-continue", Int(1)).
		Set("size", Int(1<<20)).
		Set("hash", String("aabbccddeeff00112233445566778899"))
}

func TestEncoderCoalescesFrameWrites(t *testing.T) {
	req := typicalActionRequest()

	cw := &countingWriter{}
	if err := NewEncoder(cw).Encode(req); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// The whole metadata frame must reach the conn as ONE write - the
	// unbuffered encoder issued one per tag/length/body (~30 for this frame),
	// each a syscall and a separate TLS record.
	if len(cw.sizes) != 1 {
		t.Errorf("Encode issued %d writes %v, want 1 (frame coalesced)", len(cw.sizes), cw.sizes)
	}
	got, err := NewDecoder(&cw.buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !valuesEqual(req, got) {
		t.Error("coalesced frame did not round-trip")
	}
}

func TestWriteRequestSingleWrite(t *testing.T) {
	req := typicalActionRequest()

	cw := &countingWriter{}
	if err := WriteRequest(cw, CmdAction, req); err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	// Header and frame coalesce into a single write too.
	if len(cw.sizes) != 1 {
		t.Errorf("WriteRequest issued %d writes %v, want 1 (header+frame coalesced)", len(cw.sizes), cw.sizes)
	}
	h, err := ReadHeader(&cw.buf)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Cmd != CmdAction {
		t.Errorf("Cmd = 0x%02x, want 0x%02x", h.Cmd, CmdAction)
	}
	got, err := NewDecoder(&cw.buf).Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !valuesEqual(req, got) {
		t.Error("headered frame did not round-trip")
	}
}

func TestEncoderStreamedBinaryBypassesBuffer(t *testing.T) {
	// A streamed (Reader-sourced) payload must not be buffered whole: the frame
	// prefix is flushed, then the payload is copied straight to the underlying
	// writer, then metadata buffering resumes for the trailing values.
	payload := bytes.Repeat([]byte{0xC7}, 128<<10)
	req := NewMap().
		Set("@proto", NewMap().Set("type", String("body"))).
		Set("data", Binary{Length: int64(len(payload)), Reader: bytes.NewReader(payload)}).
		Set("after", String("trailing"))

	cw := &countingWriter{}
	if err := NewEncoder(cw).Encode(req); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// First write is the flushed metadata prefix (map open, envelope, binary
	// header) - small, and crucially issued BEFORE the payload bytes.
	if len(cw.sizes) < 3 {
		t.Fatalf("writes = %v, want at least prefix+payload+trailer", cw.sizes)
	}
	if cw.sizes[0] >= 1024 {
		t.Errorf("first write is %d bytes, want a small flushed metadata prefix", cw.sizes[0])
	}
	// The payload must stream through in chunks, never as one buffered slab.
	for i, n := range cw.sizes {
		if n >= len(payload) {
			t.Errorf("write %d carried %d bytes - streamed payload was buffered whole", i, n)
		}
	}

	dec := NewDecoder(&cw.buf)
	dec.MaxInlineBinary = -1 // inline the whole payload for the comparison below
	got, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	m := got.(*Map)
	bv, _ := m.Get("data")
	b := bv.(Binary)
	if !bytes.Equal(b.Data, payload) {
		t.Error("streamed payload corrupted on the wire")
	}
	if s, ok := m.String("after"); !ok || s != "trailing" {
		t.Errorf("trailing value = %q, want %q (stream left frame misaligned)", s, "trailing")
	}
}

// stuckWriter refuses every write, standing in for a dead conn.
type stuckWriter struct{}

func (stuckWriter) Write([]byte) (int, error) { return 0, errors.New("conn is dead") }

func TestEncoderSurfacesFlushError(t *testing.T) {
	// A frame small enough to sit entirely in the buffer only touches the conn
	// at the final flush; that error must surface from Encode, not vanish -
	// callers poison the connection on it.
	if err := NewEncoder(stuckWriter{}).Encode(typicalActionRequest()); err == nil {
		t.Error("Encode on a dead writer returned nil, want error")
	}
	if err := WriteRequest(stuckWriter{}, CmdAction, typicalActionRequest()); err == nil {
		t.Error("WriteRequest on a dead writer returned nil, want error")
	}
}
