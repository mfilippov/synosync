package md4

import (
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

// Test vectors from RFC 1320, appendix A.5.
var golden = []struct {
	in  string
	out string
}{
	{"", "31d6cfe0d16ae931b73c59d7e0c089c0"},
	{"a", "bde52cb31de33e46245e05fbdbd6fb24"},
	{"abc", "a448017aaf21d8525fc10ae87aa6729d"},
	{"message digest", "d9130a8164549fe818874806e1c7014b"},
	{"abcdefghijklmnopqrstuvwxyz", "d79e1c308aa5bbcdeea8ed63df412da9"},
	{"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "043f8582f241db351ce627e153e7f0e4"},
	{"12345678901234567890123456789012345678901234567890123456789012345678901234567890", "e33b4ddc9c38f2199c3e7b164fcc0536"},
}

func TestGolden(t *testing.T) {
	for _, g := range golden {
		got := ContentHash([]byte(g.in))
		if got != g.out {
			t.Errorf("ContentHash(%q) = %s, want %s", g.in, got, g.out)
		}

		sum := Sum([]byte(g.in))
		if hex.EncodeToString(sum[:]) != g.out {
			t.Errorf("Sum(%q) = %x, want %s", g.in, sum, g.out)
		}
	}
}

// TestStreaming writes each input one byte at a time through the hash.Hash
// interface and verifies the result matches the one-shot digest.
func TestStreaming(t *testing.T) {
	for _, g := range golden {
		h := New()
		if h.Size() != Size {
			t.Errorf("Size() = %d, want %d", h.Size(), Size)
		}
		if h.BlockSize() != BlockSize {
			t.Errorf("BlockSize() = %d, want %d", h.BlockSize(), BlockSize)
		}
		for i := 0; i < len(g.in); i++ {
			n, err := h.Write([]byte{g.in[i]})
			if err != nil || n != 1 {
				t.Fatalf("Write byte %d of %q: n=%d err=%v", i, g.in, n, err)
			}
		}
		got := hex.EncodeToString(h.Sum(nil))
		if got != g.out {
			t.Errorf("streaming %q = %s, want %s", g.in, got, g.out)
		}
	}
}

// TestChunkedWrites writes inputs in chunks that straddle the 64-byte block
// boundary to exercise the buffering paths in Write.
func TestChunkedWrites(t *testing.T) {
	in := strings.Repeat("synosync", 100) // 800 bytes, > many blocks
	want := ContentHash([]byte(in))

	for _, chunk := range []int{1, 3, 7, 63, 64, 65, 100} {
		h := New()
		for off := 0; off < len(in); off += chunk {
			end := off + chunk
			if end > len(in) {
				end = len(in)
			}
			if _, err := h.Write([]byte(in[off:end])); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		got := hex.EncodeToString(h.Sum(nil))
		if got != want {
			t.Errorf("chunk=%d: got %s, want %s", chunk, got, want)
		}
	}
}

// TestSumDoesNotMutate verifies that calling Sum leaves the digest usable for
// further writes (Sum operates on a copy of the state).
func TestSumDoesNotMutate(t *testing.T) {
	h := New()
	if _, err := h.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	first := hex.EncodeToString(h.Sum(nil))
	if _, err := h.Write([]byte("c")); err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != golden[2].out { // "abc"
		t.Errorf("after second write got %s, want %s", got, golden[2].out)
	}
	if got == first {
		t.Errorf("digest unchanged after second write: %s", got)
	}
}

// TestReset verifies Reset returns the digest to its initial state.
func TestReset(t *testing.T) {
	h := New()
	if _, err := h.Write([]byte("garbage data here")); err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if _, err := h.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != golden[2].out {
		t.Errorf("after Reset got %s, want %s", got, golden[2].out)
	}
}

// TestEmpty checks the empty-input digest both one-shot and via an unwritten
// hash.
func TestEmpty(t *testing.T) {
	const want = "31d6cfe0d16ae931b73c59d7e0c089c0"
	if got := ContentHash(nil); got != want {
		t.Errorf("ContentHash(nil) = %s, want %s", got, want)
	}
	h := New()
	if got := hex.EncodeToString(h.Sum(nil)); got != want {
		t.Errorf("empty Sum = %s, want %s", got, want)
	}
}

// Ensure New() satisfies io.Writer via hash.Hash for streaming use.
var _ io.Writer = New()
