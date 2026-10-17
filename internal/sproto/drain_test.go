package sproto

// Tests for the streamed-Binary drain budget (Decoder.MaxDrainBytes). The frame budget bounds what a Decode ALLOCATES; these bound what it
// CONSUMES, so a peer declaring an absurd Binary length and drip-feeding its
// bytes cannot pin the decoding goroutine forever.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// errDripOverrun is returned by dripReader once the decoder has read further
// than any correct implementation should. It turns "the decoder drains forever"
// from a hung test into a failed assertion.
var errDripOverrun = errors.New("test: decoder read past the drain budget")

// dripReader serves prefix and then an endless trickle of payload bytes, one per
// Read - the shape of a hostile peer that keeps the connection alive while
// claiming a payload it will never finish. After limit trickled bytes it returns
// errDripOverrun rather than trickling forever.
type dripReader struct {
	prefix  []byte
	off     int
	dripped int64
	limit   int64
}

func (r *dripReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off < len(r.prefix) {
		n := copy(p, r.prefix[r.off:])
		r.off += n
		return n, nil
	}
	if r.dripped >= r.limit {
		return 0, errDripOverrun
	}
	r.dripped++
	p[0] = 0x5A
	return 1, nil
}

// binaryHeader builds the wire prefix of a plain Binary record claiming length
// bytes of payload.
func binaryHeader(length uint64) []byte {
	h := make([]byte, 9)
	h[0] = tagBinary
	binary.BigEndian.PutUint64(h[1:], length)
	return h
}

// TestDrainBudgetRejectsAbsurdBinaryLength is the red test: a Binary
// declaring 2^62 bytes used to be drained to its claimed end - the frame budget
// charges a streamed value only the 128-byte node floor, so nothing stopped the
// read. The claim must now be refused, and refused without reading the payload
// at all: what makes the drain unbounded is the claimed length, not the arrival
// rate, so an absurd claim should cost the reader nothing.
func TestDrainBudgetRejectsAbsurdBinaryLength(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stream func(Binary, io.Reader) error
	}{
		{name: "no handler"},
		{name: "handler skips payload", stream: func(Binary, io.Reader) error { return nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &dripReader{prefix: binaryHeader(1 << 62), limit: 4 << 20}
			dec := NewDecoder(src)
			dec.MaxInlineBinary = 16 // route through streamBinary
			dec.BinaryStream = tc.stream
			_, err := dec.Decode()
			if !errors.Is(err, ErrDrainTooLarge) {
				t.Fatalf("Decode error = %v, want ErrDrainTooLarge", err)
			}
			if src.dripped != 0 {
				t.Errorf("decoder consumed %d payload bytes, want 0 (the claim is refused before reading)", src.dripped)
			}
		})
	}
}

// TestDrainBudgetIsAggregate pins that the budget spans the whole Decode: a run
// of individually sub-budget claims must not outrun it either.
func TestDrainBudgetIsAggregate(t *testing.T) {
	const (
		each  = 400
		count = 3
	)
	var buf bytes.Buffer
	arr := Array{}
	for range count {
		arr = append(arr, Binary{Data: bytes.Repeat([]byte{0x11}, each)})
	}
	if err := NewEncoder(&buf).Encode(arr); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	frame := buf.Bytes()

	t.Run("aggregate past budget", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxInlineBinary = 16
		dec.MaxDrainBytes = each*count - 1
		if _, err := dec.Decode(); !errors.Is(err, ErrDrainTooLarge) {
			t.Fatalf("Decode error = %v, want ErrDrainTooLarge", err)
		}
	})
	t.Run("aggregate within budget", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxInlineBinary = 16
		dec.MaxDrainBytes = each * count
		if _, err := dec.Decode(); err != nil {
			t.Fatalf("Decode: %v, want a clean drain at exactly the budget", err)
		}
	})
	t.Run("negative opts out", func(t *testing.T) {
		dec := NewDecoder(bytes.NewReader(frame))
		dec.MaxInlineBinary = 16
		dec.MaxDrainBytes = -1
		if _, err := dec.Decode(); err != nil {
			t.Fatalf("Decode: %v, want unlimited draining under a negative budget", err)
		}
	})
}

// TestDrainBudgetIgnoresHandlerReads is the guard for the content path: the
// budget charges only what the DECODER discards, so a BinaryStream handler that
// consumes its (server-sized) payload is never charged for it. Downloads must
// keep streaming multi-GiB bodies under the default 64 MiB budget.
func TestDrainBudgetIgnoresHandlerReads(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 4096)
	var buf bytes.Buffer
	if err := NewEncoder(&buf).Encode(Binary{Data: payload}); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got bytes.Buffer
	dec := NewDecoder(&buf)
	dec.MaxInlineBinary = 16
	dec.MaxDrainBytes = 1 // far below the payload; the handler reads it all
	dec.BinaryStream = func(_ Binary, r io.Reader) error {
		_, err := io.Copy(&got, r)
		return err
	}
	if _, err := dec.Decode(); err != nil {
		t.Fatalf("Decode: %v, want the handler's own reads to be unbudgeted", err)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Errorf("handler received %d bytes, want the %d-byte payload", got.Len(), len(payload))
	}
}

// TestReadCappedResponseRefusesAbsurdBinaryClaim is the connection-level red
// test: the shared capped reply read used to discard an oversized
// Binary with an unbounded io.Copy inside its own hook, so a reply claiming 2^62
// bytes and drip-feeding them wedged the reading goroutine - and with it the
// worker, the drain batch and the whole sync cycle. The read must now return
// promptly with ErrDrainTooLarge so the caller poisons the connection and
// re-dials.
func TestReadCappedResponseRefusesAbsurdBinaryClaim(t *testing.T) {
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		// A well-formed reply map whose "data" Binary claims an absurd length,
		// followed by an endless trickle of payload bytes. The writes block until
		// the client reads them (net.Pipe is unbuffered) and fail once the client
		// end closes, so a correct decoder simply never asks for them.
		head := []byte{tagMap, tagString, 0x00, 0x04, 'd', 'a', 't', 'a'}
		head = append(head, binaryHeader(1<<62)...)
		if _, err := server.Write(head); err != nil {
			return
		}
		for {
			if _, err := server.Write([]byte{0x5A}); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})

	c := NewConn(client)
	type result struct {
		oversize bool
		err      error
	}
	res := make(chan result, 1)
	go func() {
		_, oversize, err := c.ReadCappedResponse(64)
		res <- result{oversize, err}
	}()
	select {
	case got := <-res:
		if !errors.Is(got.err, ErrDrainTooLarge) {
			t.Fatalf("ReadCappedResponse error = %v, want ErrDrainTooLarge", got.err)
		}
		if !got.oversize {
			t.Error("oversize = false, want true (the Binary did exceed the inline cap)")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ReadCappedResponse did not return: the drain is still unbounded")
	}
}

// TestReadCappedResponseDrainsWithinDrainBudget is the companion guard: a reply
// that merely overshoots the inline cap - the ordinary oversize case - is still
// drained whole, leaving the connection byte-aligned and reusable. Only an
// absurd claim is refused.
func TestReadCappedResponseDrainsWithinDrainBudget(t *testing.T) {
	const inlineCap = 64
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Comfortably past the inline cap, comfortably inside the default 64 MiB
	// drain budget.
	oversized := bytes.Repeat([]byte{0xCD}, 512<<10)
	go func() {
		enc := NewEncoder(server)
		if err := enc.Encode(NewMap().Set("errno", Int(0)).Set("data", Binary{Data: oversized})); err != nil {
			t.Errorf("server Encode reply 1: %v", err)
			return
		}
		// If the drain stopped short, this second reply decodes misaligned.
		if err := enc.Encode(NewMap().Set("errno", Int(0))); err != nil {
			t.Errorf("server Encode reply 2: %v", err)
		}
	}()

	c := NewConn(client)
	resp, oversize, err := c.ReadCappedResponse(inlineCap)
	if err != nil {
		t.Fatalf("ReadCappedResponse: %v", err)
	}
	if !oversize {
		t.Fatal("oversize = false, want true")
	}
	m, ok := resp.(*Map)
	if !ok {
		t.Fatalf("resp is %T, want *Map", resp)
	}
	v, _ := m.Get("data")
	b, ok := v.(Binary)
	if !ok {
		t.Fatalf("data is %T, want Binary", v)
	}
	if b.Data != nil || b.Length != int64(len(oversized)) {
		t.Errorf("Binary = {len(Data)=%d, Length=%d}, want {nil, %d}", len(b.Data), b.Length, len(oversized))
	}
	if _, oversize2, err := c.ReadCappedResponse(inlineCap); err != nil || oversize2 {
		t.Fatalf("second read after drain: err = %v, oversize = %v; want nil, false (connection aligned)", err, oversize2)
	}
}

// TestDrainBudgetDefaultApplies pins the zero-value semantics: a bare Decoder is
// budgeted at defaultMaxDrainBytes rather than draining without limit.
func TestDrainBudgetDefaultApplies(t *testing.T) {
	src := &dripReader{prefix: binaryHeader(defaultMaxDrainBytes + 1), limit: 1 << 20}
	dec := NewDecoder(src)
	dec.MaxInlineBinary = 16
	if _, err := dec.Decode(); !errors.Is(err, ErrDrainTooLarge) {
		t.Fatalf("Decode error = %v, want ErrDrainTooLarge at the default budget", err)
	}
	if src.dripped != 0 {
		t.Errorf("decoder consumed %d payload bytes, want 0", src.dripped)
	}
}
