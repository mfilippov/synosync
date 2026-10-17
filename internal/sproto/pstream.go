package sproto

// This file implements the recursive PStream/PObject streaming codec used for
// every message after the TLS upgrade. A PObject is a tree of values encoded as
// a sequence of tag-length-value records, big-endian throughout:
//
//	0x00  Null     [0x00][0x00]
//	0x01  Int      [0x01][size:u8][value:size bytes big-endian]   size ∈ {1,2,4,8}
//	0x10  String   [0x10][len:u16 big-endian][raw bytes]
//	0x30  Binary   [0x30][len:u64 big-endian][raw bytes]          (file content; streamed)
//	0x40  End      [0x40]                                         (terminates a map/array)
//	0x41  Array    [0x41][value*][0x40]
//	0x42  Map      [0x42][(stringKey value)*][0x40]
//	0x43  BinaryEx [0x43][map{binary:bin, send_hash:str}][0x40]   (hash-checked binary)
//
// Two framing conventions sit on top of the same codec: client requests are
// prefixed with the 8-byte Header (see header.go), while server responses after
// the handshake are "bare" PObjects with no header. WriteRequest/ReadResponse
// expose both.
//
// Map keys whose canonical name begins with a single underscore lose it on the
// wire (the server's "_action" is written as "action"); the "@" prefix of the
// "@proto" envelope is preserved. The encoder applies the strip; the decoder
// therefore only ever sees stripped keys.

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// PStream value tags, as they appear on the wire.
const (
	tagNull     = 0x00
	tagInt      = 0x01
	tagString   = 0x10
	tagBinary   = 0x30
	tagEnd      = 0x40
	tagArray    = 0x41
	tagMap      = 0x42
	tagBinaryEx = 0x43
)

// maxDepth bounds how deeply nested an Array/Map tree the decoder will follow.
// Each level costs a Go stack frame, so an unbounded run of array/map-open tags
// from the peer would otherwise grow the goroutine stack until the runtime
// fatally aborts the process. Real messages nest only a handful of levels; 1000
// leaves enormous headroom while turning a hostile or corrupt frame into a
// recoverable error.
const maxDepth = 1000

// maxNodes bounds how many values a single Decode will materialise. maxDepth
// only caps nesting; a *flat* array or map of tiny tags (a 2-byte Null or an
// empty Array per element) still grows an unbounded []Value/map. Because Conn
// reads straight from the socket with no frame-size limit, a peer could stream
// such a run and drive the decoder to allocate without bound. Bulk data is
// meant to ride in streamed Binary values (see MaxInlineBinary), so structural
// nodes stay modest; 1<<20 (~1M values in a single frame) leaves vast headroom
// for real listings while turning a hostile or corrupt frame into a recoverable
// error instead of an OOM kill.
//
// It is a backstop rather than the primary breadth guard: every node also pays
// nodeCostBytes against the frame budget, which at the default MaxFrameBytes
// trips first (64 MiB / 128 B = 512Ki nodes). A caller that raises
// MaxFrameBytes still cannot raise breadth past this count.
const maxNodes = 1 << 20

// nodeCostBytes is the fixed frame-budget price of materialising one decoded
// value, charged in ADDITION to nothing and DISCOUNTED from that value's own
// payload charge - so a node costs max(wire bytes, nodeCostBytes) of the
// budget (see chargeBytes/chargePayload).
//
// Why a floor at all: a node's wire footprint is a terrible proxy for its heap
// footprint. An empty Map is 2 bytes on the wire but ~96 bytes in the heap (the
// Map struct, its runtime map, and the interface slot holding it in the
// parent), and a decoded page is then re-materialised by consumers that
// pre-size from the decoded length (actions' decodeEvents/decodeChanges/List/
// QueryUserInfo do make(..., len(arr))). Charging payload bytes only, a ~2 MB
// frame of ~1M empty Maps fit inside both MaxFrameBytes and maxNodes while
// costing ~100 MB in the decoder plus as much again downstream - a ~100×
// amplification that OOMs a small NAS-side host.
//
// Why 128: it approximates the real per-node heap cost (interface slot 16 B
// plus the value's own allocation; an empty Map ~96 B, a populated map's entry
// with its key ~48 B), so the budget now means what it says - 64 MiB of budget
// buys ~64 MiB of decoder heap instead of ~3 GB. The measured price of a fat
// ten-field catalog/event entry is ~1.5 KiB (TestFrameBudgetRealisticPagesFit),
// so one reply may still carry ~45k entries - orders of magnitude above what a
// live DSM pages, though roughly half the ~95k that maxNodes alone allowed.
// That halving is the accepted trade: an over-large reply degrades to a
// recoverable ErrFrameTooLarge, whereas the old accounting degraded to an OOM
// kill on a small NAS-side host.
const nodeCostBytes = 128

// binaryAllocHint caps the initial capacity reserved for an inline Binary
// payload. The true size is read incrementally so a forged 64-bit length field
// cannot force a multi-gigabyte up-front allocation; legitimate larger payloads
// still grow the buffer as their bytes actually arrive.
const binaryAllocHint = 1 << 20 // 1 MiB

// defaultMaxInlineBinary is the inline-Binary cap applied when
// Decoder.MaxInlineBinary is zero. A zero cap used to mean "inline without
// limit", which left every call site responsible for remembering its own cap -
// wrong-way-around for hostile input. 1 MiB matches the caps the production
// call sites already choose (actions/auth), so a bare Decoder is safe by
// construction; a caller that genuinely needs unlimited inlining opts out
// explicitly with a negative MaxInlineBinary.
const defaultMaxInlineBinary = 1 << 20 // 1 MiB

// defaultMaxFrameBytes bounds the total in-memory cost a single Decode will
// buffer across all values in one frame: payload bytes plus the per-node floor
// (see nodeCostBytes). maxNodes caps how many values a frame holds and
// MaxInlineBinary caps any single Binary, but a flat array of many sub-cap
// Binary (or String) values could still sum to an unbounded allocation -
// neither per-value nor per-count guard sees the aggregate. This budget turns
// that into a recoverable ErrFrameTooLarge. 64 MiB leaves vast headroom for
// real control replies (file content rides in streamed Binary values, which are
// drained rather than buffered and so do not count) while stopping a hostile or
// corrupt frame before it OOMs.
const defaultMaxFrameBytes = 64 << 20 // 64 MiB

// defaultMaxDrainBytes bounds how many payload bytes a single Decode will
// DISCARD on behalf of streamed Binary values when Decoder.MaxDrainBytes is
// zero. It closes the one axis the frame budget deliberately does not cover:
// a streamed Binary is never buffered, so it pays only the nodeCostBytes floor
// against MaxFrameBytes - which bounds allocation but not CONSUMPTION. Without
// a drain budget the codec reads to the server-claimed length, up to 2^63-1
// bytes, so a hostile or buggy peer that declares a 2^62-byte Binary and then
// drips its bytes pins the reading goroutine forever: neither the frame budget
// (128 B charged) nor an idle-progress watchdog (bytes ARE arriving) trips, and
// the codec's contract - a hostile frame degrades to a recoverable error - is
// broken on the byte-consumption axis.
//
// 64 MiB matches defaultMaxFrameBytes, so every hostile frame shape now costs
// the peer at most the same order of input before it is rejected. It sits three
// orders of magnitude above defaultMaxInlineBinary (1 MiB), so a legitimate
// reply that merely overshoots the inline cap is still drained and the
// connection stays byte-aligned and reusable - only an absurd claim is refused.
// Content transfers are unaffected: the download path consumes its payload
// through BinaryStream rather than leaving it to the drain.
const defaultMaxDrainBytes = 64 << 20 // 64 MiB

// ErrUnexpectedEnd is returned when an End tag (0x40) appears where a value was
// expected, e.g. as a top-level object or as a map key.
var ErrUnexpectedEnd = errors.New("sproto: unexpected end marker")

// ErrFrameTooLarge is returned when the total in-memory payload bytes a single
// Decode would buffer exceeds the frame budget (see Decoder.MaxFrameBytes).
var ErrFrameTooLarge = errors.New("sproto: frame payload exceeds budget")

// ErrDrainTooLarge is returned when the payload bytes a single Decode would
// discard for streamed Binary values exceed the drain budget (see
// Decoder.MaxDrainBytes). Unlike an oversized-but-drained Binary - which leaves
// the connection byte-aligned - this aborts mid-frame, so the caller must treat
// the connection as poisoned and re-dial.
var ErrDrainTooLarge = errors.New("sproto: drained binary payload exceeds budget")

// Value is one node of a decoded PObject tree. The concrete types are Null,
// Int, String, Binary, Array and *Map.
type Value interface {
	isValue()
}

// Null is the absence of a value (wire tag 0x00).
type Null struct{}

// Int is a signed integer value (wire tag 0x01). On the wire it is encoded with
// the smallest of 1, 2, 4 or 8 big-endian bytes that holds the magnitude;
// negative values always use the full 8 bytes (two's complement).
type Int int64

// String is a length-prefixed byte string (wire tag 0x10). It may hold
// arbitrary bytes, not only valid UTF-8.
type String string

// Binary is a raw binary value: a plain 0x30 record, or - when SendHash is set
// - a 0x43 BinaryEx record that pairs the bytes with an integrity hash. Large
// values can be produced from Reader and consumed via Decoder streaming rather
// than being held wholly in memory.
type Binary struct {
	// Length is the number of payload bytes. When encoding from Reader it is
	// authoritative; when encoding from Data it is ignored (len(Data) is used).
	Length int64
	// SendHash, when non-empty, selects the BinaryEx (0x43) framing and carries
	// the content hash the peer should verify.
	SendHash string
	// Data holds the inline payload. It is nil for values that were streamed
	// past during decoding (see Decoder.MaxInlineBinary).
	Data []byte
	// Reader, when non-nil, is the streaming source used during encoding; exactly
	// Length bytes are read from it. It takes precedence over Data.
	Reader io.Reader
}

// Array is an ordered list of values (wire tag 0x41).
type Array []Value

func (Null) isValue()   {}
func (Int) isValue()    {}
func (String) isValue() {}
func (Binary) isValue() {}
func (Array) isValue()  {}
func (*Map) isValue()   {}

// Map is an insertion-ordered set of string-keyed values (wire tag 0x42). Order
// is preserved because the wire format is order-sensitive and deterministic
// round-tripping depends on it.
type Map struct {
	keys  []string
	byKey map[string]Value
}

// NewMap returns an empty Map ready for Set.
func NewMap() *Map {
	return &Map{byKey: make(map[string]Value)}
}

// Set assigns value to key, preserving first-insertion order. Re-setting an
// existing key overwrites its value without changing its position.
func (m *Map) Set(key string, value Value) *Map {
	if m.byKey == nil {
		m.byKey = make(map[string]Value)
	}
	if _, ok := m.byKey[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.byKey[key] = value
	return m
}

// Get returns the value stored under key and whether it was present.
func (m *Map) Get(key string) (Value, bool) {
	v, ok := m.byKey[key]
	return v, ok
}

// Int returns the integer stored under key and whether it was present as an Int.
// A missing key or a present-but-wrong-typed value reports (0, false).
func (m *Map) Int(key string) (int64, bool) {
	v, ok := m.byKey[key]
	if !ok {
		return 0, false
	}
	iv, ok := v.(Int)
	if !ok {
		return 0, false
	}
	return int64(iv), true
}

// String returns the string stored under key and whether it was present as a
// String. A missing key or a present-but-wrong-typed value reports ("", false).
func (m *Map) String(key string) (string, bool) {
	v, ok := m.byKey[key]
	if !ok {
		return "", false
	}
	sv, ok := v.(String)
	if !ok {
		return "", false
	}
	return string(sv), true
}

// Keys returns the keys in insertion order. The returned slice must not be
// mutated.
func (m *Map) Keys() []string { return m.keys }

// Len reports the number of entries.
func (m *Map) Len() int { return len(m.keys) }

// stripKey removes a single leading underscore from a wire key. The "@" prefix
// of "@proto" and other keys are left untouched.
func stripKey(key string) string {
	if len(key) > 0 && key[0] == '_' {
		return key[1:]
	}
	return key
}

// encoderBufSize is the size of the per-Encoder frame buffer. A PObject frame
// is mostly 1-3-byte metadata writes (tags, length prefixes, small scalars);
// unbuffered, each was a separate syscall - and on a tls.Conn a separate TLS
// record with ~29 bytes of overhead. Coalescing them costs one 4 KiB buffer per
// Encoder and turns a typical request into a single Write.
const encoderBufSize = 4096

// Encoder writes PObject values to an io.Writer. Frame metadata is coalesced
// into an internal buffer and flushed once per Encode, so a typical request
// costs one Write on the underlying writer instead of dozens of tiny ones.
// Streamed Binary payloads (Binary.Reader) are still copied straight to the
// underlying writer - bulk bytes are never buffered here - and large inline
// Data rides bufio's large-write bypass. On an encode error the buffered
// remainder is NOT flushed: callers treat any encode error as poisoning the
// connection, and buffering only narrows the partial-frame window they must
// already assume.
type Encoder struct {
	// w is the underlying writer. Streamed Binary payloads are copied straight
	// to it (after a flush) so bulk data is not re-chunked through the frame
	// buffer.
	w io.Writer
	// bw coalesces the frame's metadata writes over w; Encode flushes it once
	// per frame on success.
	bw *bufio.Writer
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, bw: bufio.NewWriterSize(w, encoderBufSize)}
}

// Encode writes the wire encoding of v, flushing the whole frame to the
// underlying writer before returning. On error nothing more is flushed; the
// stream may hold a partial frame, exactly as with unbuffered encoding.
func (e *Encoder) Encode(v Value) error {
	if err := e.encodeValue(v); err != nil {
		return err
	}
	return e.bw.Flush()
}

func (e *Encoder) writeByte(b byte) error {
	return e.bw.WriteByte(b)
}

func (e *Encoder) encodeValue(v Value) error {
	switch val := v.(type) {
	case Null:
		_, err := e.bw.Write([]byte{tagNull, 0x00})
		return err
	case Int:
		return e.encodeInt(int64(val))
	case String:
		return e.encodeString(tagString, string(val))
	case Binary:
		return e.encodeBinary(val)
	case Array:
		if err := e.writeByte(tagArray); err != nil {
			return err
		}
		for _, el := range val {
			if err := e.encodeValue(el); err != nil {
				return err
			}
		}
		return e.writeByte(tagEnd)
	case *Map:
		// A typed-nil *Map (e.g. a nil variable of that type assigned into a Value)
		// is a non-nil interface, so it reaches this case rather than the nil one
		// below and would panic dereferencing m.keys deep inside a request builder.
		// Report it as an error like every other unencodable value: a malformed
		// request must fail the turn, never take the daemon down.
		if val == nil {
			return errors.New("sproto: cannot encode nil *Map")
		}
		return e.encodeMap(val)
	case nil:
		return errors.New("sproto: cannot encode nil value")
	default:
		return fmt.Errorf("sproto: cannot encode value of type %T", v)
	}
}

func (e *Encoder) encodeMap(m *Map) error {
	if err := e.writeByte(tagMap); err != nil {
		return err
	}
	for _, key := range m.keys {
		if err := e.encodeString(tagString, stripKey(key)); err != nil {
			return err
		}
		if err := e.encodeValue(m.byKey[key]); err != nil {
			return err
		}
	}
	return e.writeByte(tagEnd)
}

func (e *Encoder) encodeInt(v int64) error {
	size := intSize(v)
	buf := make([]byte, 2+size)
	buf[0] = tagInt
	buf[1] = byte(size & 0xff)
	u := uint64(v) // #nosec G115 -- intentional two's-complement reinterpretation for big-endian emission
	for i := size - 1; i >= 0; i-- {
		buf[2+i] = byte(u)
		u >>= 8
	}
	_, err := e.bw.Write(buf)
	return err
}

// intSize returns the number of big-endian bytes used to encode v: the smallest
// of 1, 2, 4 or 8 that holds its unsigned magnitude, or 8 for negative values.
func intSize(v int64) int {
	if v < 0 {
		return 8
	}
	switch u := uint64(v); {
	case u <= 0xFF:
		return 1
	case u <= 0xFFFF:
		return 2
	case u <= 0xFFFFFFFF:
		return 4
	default:
		return 8
	}
}

func (e *Encoder) encodeString(tag byte, s string) error {
	if len(s) > 0xFFFF {
		return fmt.Errorf("sproto: string too long: %d bytes (max %d)", len(s), 0xFFFF)
	}
	hdr := []byte{tag, 0, 0}
	binary.BigEndian.PutUint16(hdr[1:3], uint16(len(s))) // #nosec G115 -- length bounded by the 0xFFFF check above
	if _, err := e.bw.Write(hdr); err != nil {
		return err
	}
	_, err := e.bw.WriteString(s)
	return err
}

func (e *Encoder) encodeBinary(b Binary) error {
	if b.SendHash != "" {
		// BinaryEx (0x43): a map of {"binary": <0x30 bytes>, "send_hash": str}.
		if err := e.writeByte(tagBinaryEx); err != nil {
			return err
		}
		if err := e.encodeString(tagString, "binary"); err != nil {
			return err
		}
		if err := e.encodeRawBinary(b); err != nil {
			return err
		}
		if err := e.encodeString(tagString, "send_hash"); err != nil {
			return err
		}
		if err := e.encodeString(tagString, b.SendHash); err != nil {
			return err
		}
		return e.writeByte(tagEnd)
	}
	return e.encodeRawBinary(b)
}

// encodeRawBinary writes a plain 0x30 binary record, streaming from b.Reader
// when set or copying b.Data otherwise.
func (e *Encoder) encodeRawBinary(b Binary) error {
	length := b.Length
	if b.Reader == nil {
		length = int64(len(b.Data))
	}
	if length < 0 {
		return fmt.Errorf("sproto: negative binary length %d", length)
	}
	hdr := make([]byte, 9)
	hdr[0] = tagBinary
	binary.BigEndian.PutUint64(hdr[1:9], uint64(length)) // #nosec G115 -- length is non-negative (checked above)
	if _, err := e.bw.Write(hdr); err != nil {
		return err
	}
	if b.Reader != nil {
		// Flush the buffered frame prefix, then stream the payload straight to
		// the underlying writer: bulk bytes must be neither buffered here nor
		// re-chunked through the small frame buffer.
		if err := e.bw.Flush(); err != nil {
			return err
		}
		n, err := io.CopyN(e.w, b.Reader, length)
		if err != nil {
			return fmt.Errorf("sproto: writing binary payload (%d/%d bytes): %w", n, length, err)
		}
		return nil
	}
	// Inline data goes through the frame buffer; a payload larger than the
	// buffer takes bufio's large-write bypass, so it is not copied byte-by-byte.
	_, err := e.bw.Write(b.Data)
	return err
}

// Decoder reads PObject values from an io.Reader.
type Decoder struct {
	r io.Reader
	// MaxInlineBinary caps how many payload bytes of a Binary value are buffered
	// into Binary.Data. Larger values are not held in memory: their bytes are
	// handed to BinaryStream (if set) or discarded, and Data is left nil while
	// Length still reports the true size. Zero selects defaultMaxInlineBinary
	// (1 MiB) so a bare Decoder is hostile-input-safe by construction; a
	// negative value opts out of the cap and inlines without limit (the frame
	// budget still applies - see MaxFrameBytes).
	MaxInlineBinary int64
	// BinaryStream, when set, receives the payload of every Binary value too
	// large to inline. The reader yields exactly Length bytes; any bytes left
	// unread when it returns are drained so the following value stays aligned.
	BinaryStream func(b Binary, payload io.Reader) error
	// MaxFrameBytes caps the total in-memory cost a single Decode will buffer
	// across the whole frame: payload bytes (inline Binary Data plus String
	// bodies) and, for every materialised value, a nodeCostBytes floor standing
	// in for its structural heap footprint - a value costs max(wire,
	// nodeCostBytes). Zero selects defaultMaxFrameBytes. Streamed Binary values
	// (those over MaxInlineBinary) are drained rather than buffered and so are
	// charged the node floor only; their CONSUMPTION is bounded separately by
	// MaxDrainBytes.
	MaxFrameBytes int64
	// MaxDrainBytes caps the total payload bytes a single Decode will discard on
	// behalf of streamed Binary values: the bytes a BinaryStream handler leaves
	// unread, and - when no handler is set - the whole payload. It bounds
	// consumption where MaxFrameBytes bounds allocation, so a peer cannot pin the
	// decoding goroutine on an unbounded discard loop by declaring an absurd
	// Binary length and drip-feeding its bytes. Zero selects defaultMaxDrainBytes
	// (64 MiB); a negative value opts out and drains without limit. A value whose
	// unread remainder exceeds the budget is refused with ErrDrainTooLarge BEFORE
	// its bytes are read, which aborts the frame - the connection is left
	// misaligned and the caller must re-dial.
	//
	// It does not bound what a BinaryStream handler chooses to read: content
	// transfers are server-sized by design and guard themselves (the download
	// path arms an idle-progress watchdog).
	MaxDrainBytes int64
	// depth tracks the current Array/Map nesting level during a Decode, guarding
	// against unbounded recursion from a hostile or corrupt stream.
	depth int
	// nodes counts the values materialised in the current Decode, guarding
	// against unbounded breadth (a flat run of tiny tags). Reset by Decode.
	nodes int
	// frameBytes accumulates the buffered payload bytes of the current Decode,
	// guarding against unbounded aggregate allocation. Reset by Decode.
	frameBytes int64
	// drainBytes accumulates the payload bytes discarded for streamed Binary
	// values in the current Decode, guarding against unbounded aggregate
	// consumption (a flat run of sub-budget claims must not outrun the budget
	// either). Reset by Decode.
	drainBytes int64
}

// NewDecoder returns a Decoder reading from r.
func NewDecoder(r io.Reader) *Decoder { return &Decoder{r: r} }

// Decode reads and returns a single top-level PObject value.
func (d *Decoder) Decode() (Value, error) {
	// depth is self-balancing (readValue increments on entry and decrements via
	// defer, so it returns to 0 on every clean or error path), but reset it here
	// for symmetry with nodes/frameBytes and as defense against a future code
	// path that leaves it unbalanced: a stale positive depth would otherwise
	// shrink the effective nesting budget of the next Decode on a reused decoder.
	d.depth = 0
	d.nodes = 0
	d.frameBytes = 0
	d.drainBytes = 0
	tag, err := d.readTag()
	if err != nil {
		return nil, err
	}
	return d.readValue(tag)
}

// chargeBytes adds n to the running frame-byte total, rejecting with
// ErrFrameTooLarge any n that would push the aggregate past the budget. It is
// called before an allocation so an oversized claim errors out instead of
// buffering.
//
// Division of responsibility between the OOM guards: chargeBytes/MaxFrameBytes
// bound the aggregate in-memory cost of a frame - buffered payload (inline
// Binary Data and String bodies) plus the nodeCostBytes floor every readValue
// charges for the value's own structural heap footprint. Breadth is ALSO
// bounded by the nodes/maxNodes counter and depth by depth/maxDepth, but those
// are now backstops: at the default budget the byte guard trips first for tiny
// nodes, which is the point - maxNodes alone let ~1M two-byte nodes cost ~100 MB.
func (d *Decoder) chargeBytes(n int64) error {
	budget := d.MaxFrameBytes
	if budget <= 0 {
		budget = defaultMaxFrameBytes
	}
	// Compare against the remaining headroom instead of add-then-check: n is
	// never negative (string lengths are u16, binary lengths are validated
	// non-negative) and frameBytes never exceeds budget, so budget-d.frameBytes
	// cannot underflow - whereas d.frameBytes+n would wrap negative on a forged
	// length near 1<<63-1 once at least one byte was already charged, slipping
	// past an add-then-check guard onto the buffering path.
	if n > budget-d.frameBytes {
		return fmt.Errorf("sproto: frame payload exceeds %d bytes: %w", budget, ErrFrameTooLarge)
	}
	d.frameBytes += n
	return nil
}

// chargePayload charges n payload bytes for a value that has already been
// charged the nodeCostBytes floor, so the value costs max(n, nodeCostBytes)
// rather than their sum. A payload smaller than the floor is already paid for.
func (d *Decoder) chargePayload(n int64) error {
	if n <= nodeCostBytes {
		return nil
	}
	return d.chargeBytes(n - nodeCostBytes)
}

func (d *Decoder) readTag() (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(d.r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// readValue decodes the value introduced by tag.
func (d *Decoder) readValue(tag byte) (Value, error) {
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > maxDepth {
		return nil, fmt.Errorf("sproto: nesting depth exceeds %d", maxDepth)
	}
	d.nodes++
	if d.nodes > maxNodes {
		return nil, fmt.Errorf("sproto: value count exceeds %d", maxNodes)
	}
	// Charge the value's structural heap footprint before decoding it: its wire
	// size understates it by up to ~50× for tiny nodes (see nodeCostBytes), so
	// without this a cheap frame of many empty Maps buys an expensive heap. The
	// value's own payload charge is discounted by this floor (chargePayload).
	if err := d.chargeBytes(nodeCostBytes); err != nil {
		return nil, err
	}
	switch tag {
	case tagNull:
		// Null is two bytes: the tag and a trailing zero. A non-zero trailer
		// signals a malformed frame that would otherwise desync the stream.
		trailer, err := d.readTag()
		if err != nil {
			return nil, err
		}
		if trailer != 0x00 {
			return nil, fmt.Errorf("sproto: null trailer is 0x%02x, want 0x00", trailer)
		}
		return Null{}, nil
	case tagInt:
		return d.readInt()
	case tagString:
		s, err := d.readStringBody(true)
		if err != nil {
			return nil, err
		}
		return String(s), nil
	case tagBinary:
		return d.readBinary()
	case tagArray:
		return d.readArray()
	case tagMap:
		return d.readMap()
	case tagBinaryEx:
		return d.readBinaryEx()
	case tagEnd:
		return nil, ErrUnexpectedEnd
	default:
		return nil, fmt.Errorf("sproto: unknown value tag 0x%02x", tag)
	}
}

func (d *Decoder) readInt() (Value, error) {
	size, err := d.readTag()
	if err != nil {
		return nil, err
	}
	if size != 1 && size != 2 && size != 4 && size != 8 {
		return nil, fmt.Errorf("sproto: invalid integer size %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return nil, err
	}
	var u uint64
	for _, b := range buf {
		u = u<<8 | uint64(b)
	}
	return Int(int64(u)), nil // #nosec G115 -- two's-complement reinterpretation of the decoded magnitude
}

// readStringBody reads a u16-length-prefixed string body. discountNode says the
// caller already charged this value the nodeCostBytes floor - true for a String
// VALUE (readValue charged it), false for a map key, which is not a node of its
// own and so pays its bytes in full. A key's own map-entry overhead is left
// uncharged: every key is followed by a value node paying the full floor, so key
// count cannot outrun the budget on its own.
func (d *Decoder) readStringBody(discountNode bool) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(d.r, lenBuf[:]); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 {
		return "", nil
	}
	charge := d.chargeBytes
	if discountNode {
		charge = d.chargePayload
	}
	if err := charge(int64(n)); err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(d.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (d *Decoder) readArray() (Value, error) {
	arr := Array{}
	for {
		tag, err := d.readTag()
		if err != nil {
			return nil, err
		}
		if tag == tagEnd {
			return arr, nil
		}
		el, err := d.readValue(tag)
		if err != nil {
			return nil, err
		}
		arr = append(arr, el)
	}
}

func (d *Decoder) readMap() (Value, error) {
	m := NewMap()
	for {
		tag, err := d.readTag()
		if err != nil {
			return nil, err
		}
		if tag == tagEnd {
			return m, nil
		}
		if tag != tagString {
			return nil, fmt.Errorf("sproto: map key has tag 0x%02x, want string", tag)
		}
		key, err := d.readStringBody(false)
		if err != nil {
			return nil, err
		}
		valTag, err := d.readTag()
		if err != nil {
			return nil, err
		}
		val, err := d.readValue(valTag)
		if err != nil {
			return nil, err
		}
		m.Set(key, val)
	}
}

// readBinary decodes a plain 0x30 record, inlining or streaming the payload per
// MaxInlineBinary.
func (d *Decoder) readBinary() (Value, error) {
	var lenBuf [8]byte
	if _, err := io.ReadFull(d.r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int64(binary.BigEndian.Uint64(lenBuf[:])) // #nosec G115 -- overflow detected by the length < 0 check below
	if length < 0 {
		return nil, fmt.Errorf("sproto: binary length overflows int64")
	}
	b := Binary{Length: length}
	inlineCap := d.MaxInlineBinary
	if inlineCap == 0 {
		inlineCap = defaultMaxInlineBinary
	}
	if inlineCap > 0 && length > inlineCap {
		if err := d.streamBinary(b); err != nil {
			return nil, err
		}
		return b, nil
	}
	// This value is about to be buffered in memory: charge it against the frame
	// budget before allocating so an aggregate of sub-cap values cannot OOM. The
	// node floor readValue already charged covers the first nodeCostBytes.
	if err := d.chargePayload(length); err != nil {
		return nil, err
	}
	// Read incrementally rather than pre-allocating the claimed length: a forged
	// 64-bit length must otherwise materialise as a single make() before the
	// missing bytes are noticed. io.Copy grows the buffer as bytes truly arrive,
	// so a short stream yields a clean ErrUnexpectedEOF instead of an OOM.
	buf := bytes.NewBuffer(make([]byte, 0, min(length, binaryAllocHint)))
	got, err := io.Copy(buf, io.LimitReader(d.r, length))
	if err != nil {
		return nil, err
	}
	if got != length {
		return nil, io.ErrUnexpectedEOF
	}
	b.Data = buf.Bytes()
	return b, nil
}

// streamBinary consumes length bytes for an oversized Binary, routing them to
// BinaryStream when set and discarding any remainder under the drain budget.
func (d *Decoder) streamBinary(b Binary) error {
	lr := &io.LimitedReader{R: d.r, N: b.Length}
	if d.BinaryStream != nil {
		if err := d.BinaryStream(b, lr); err != nil {
			return err
		}
	}
	// Drain whatever the handler left so the next value stays aligned.
	if err := d.drainRemainder(lr); err != nil {
		return err
	}
	// A premature EOF leaves bytes still owed: io.Copy reports success on a
	// short underlying stream, so treat an unmet remainder as truncation
	// rather than silently accepting an incomplete payload.
	if lr.N > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// drainRemainder discards the bytes a BinaryStream handler left unread (or the
// whole payload when no handler is set), charging them against the Decode's
// drain budget. A remainder past the budget is refused BEFORE any of it is read:
// the whole point is that the claimed length - not the arrival rate - is what
// makes the drain unbounded, so an absurd claim must cost the reader nothing.
//
// Aborting mid-frame leaves the connection byte-misaligned, which is the correct
// trade: the alternative is a goroutine that never returns. The reply carrying
// such a Binary is rejected by every caller anyway (ErrUnexpectedReply), so the
// fix converts an aligned rejection into a poisoned one - the cost is a single
// re-dial.
func (d *Decoder) drainRemainder(lr *io.LimitedReader) error {
	budget := d.MaxDrainBytes
	if budget == 0 {
		budget = defaultMaxDrainBytes
	}
	if budget > 0 {
		// budget-d.drainBytes cannot underflow: drainBytes only ever grows by an
		// amount that fits the remaining headroom. Comparing against that headroom
		// rather than adding first also keeps a forged length near 1<<63-1 from
		// wrapping negative and slipping past the guard.
		if lr.N > budget-d.drainBytes {
			return fmt.Errorf("sproto: binary payload drain of %d bytes exceeds the %d-byte budget: %w",
				lr.N, budget, ErrDrainTooLarge)
		}
	}
	n, err := io.Copy(io.Discard, lr)
	d.drainBytes += n
	return err
}

// readBinaryEx decodes a 0x43 record into a Binary, flattening the inner
// {"binary", "send_hash"} map.
func (d *Decoder) readBinaryEx() (Value, error) {
	var b Binary
	var gotBinary bool
	for {
		tag, err := d.readTag()
		if err != nil {
			return nil, err
		}
		if tag == tagEnd {
			if !gotBinary {
				return nil, errors.New("sproto: binaryex without binary value")
			}
			return b, nil
		}
		if tag != tagString {
			return nil, fmt.Errorf("sproto: binaryex key has tag 0x%02x, want string", tag)
		}
		key, err := d.readStringBody(false)
		if err != nil {
			return nil, err
		}
		valTag, err := d.readTag()
		if err != nil {
			return nil, err
		}
		val, err := d.readValue(valTag)
		if err != nil {
			return nil, err
		}
		switch key {
		case "binary":
			bin, ok := val.(Binary)
			if !ok {
				return nil, fmt.Errorf("sproto: binaryex binary field is %T, want Binary", val)
			}
			b.Length = bin.Length
			b.Data = bin.Data
			gotBinary = true
		case "send_hash":
			s, ok := val.(String)
			if !ok {
				return nil, fmt.Errorf("sproto: binaryex send_hash field is %T, want String", val)
			}
			b.SendHash = string(s)
		}
	}
}

// WriteRequest writes a client request: the 8-byte frame header for cmd
// followed by the wire encoding of v. The header is written through the
// encoder's frame buffer so header and frame coalesce into a single Write on
// the underlying writer for a typical (small, non-streamed) request.
func WriteRequest(w io.Writer, cmd uint8, v Value) error {
	e := NewEncoder(w)
	if _, err := (Header{Cmd: cmd}).WriteTo(e.bw); err != nil {
		return err
	}
	return e.Encode(v)
}

// ReadResponse reads a bare server-response PObject - one with no frame header,
// as the server sends after the handshake.
func ReadResponse(r io.Reader) (Value, error) {
	return NewDecoder(r).Decode()
}
