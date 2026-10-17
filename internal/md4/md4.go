// Package md4 implements the MD4 message-digest algorithm as defined in
// RFC 1320. It is a self-contained, pure-Go implementation in the style of the
// standard library hash packages and satisfies hash.Hash.
//
// MD4 is cryptographically broken and must not be used for security purposes.
// synosync needs it only because the Synology Drive wire protocol identifies
// file contents by their lowercase-hex MD4 digest.
package md4

import (
	"encoding/hex"
	"hash"
)

// The size of an MD4 checksum in bytes.
const Size = 16

// The blocksize of MD4 in bytes.
const BlockSize = 64

// Initial digest values (RFC 1320, section 3.3).
const (
	initA = 0x67452301
	initB = 0xefcdab89
	initC = 0x98badcfe
	initD = 0x10325476
)

// digest represents the partial evaluation of a checksum.
type digest struct {
	s   [4]uint32
	x   [BlockSize]byte
	nx  int
	len uint64
}

// New returns a new hash.Hash computing the MD4 checksum.
func New() hash.Hash {
	d := new(digest)
	d.Reset()
	return d
}

func (d *digest) Reset() {
	d.s[0] = initA
	d.s[1] = initB
	d.s[2] = initC
	d.s[3] = initD
	d.nx = 0
	d.len = 0
}

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return BlockSize }

func (d *digest) Write(p []byte) (n int, err error) {
	n = len(p)
	d.len += uint64(n)
	if d.nx > 0 {
		k := copy(d.x[d.nx:], p)
		d.nx += k
		if d.nx == BlockSize {
			block(d, d.x[:])
			d.nx = 0
		}
		p = p[k:]
	}
	if len(p) >= BlockSize {
		k := len(p) &^ (BlockSize - 1)
		block(d, p[:k])
		p = p[k:]
	}
	if len(p) > 0 {
		d.nx = copy(d.x[:], p)
	}
	return n, nil
}

func (d *digest) Sum(in []byte) []byte {
	// Operate on a copy so that Sum does not change the underlying state and
	// callers may keep writing afterwards.
	d0 := *d
	hash := d0.checkSum()
	return append(in, hash[:]...)
}

func (d *digest) checkSum() [Size]byte {
	// Padding: append a single 0x80 byte, then zeros, leaving room for the
	// 8-byte little-endian length so the message becomes a multiple of 64.
	len := d.len
	var tmp [64]byte
	tmp[0] = 0x80
	// digest.Write never returns an error, so the results are discarded.
	if len%64 < 56 {
		_, _ = d.Write(tmp[0 : 56-len%64])
	} else {
		_, _ = d.Write(tmp[0 : 64+56-len%64])
	}

	// Append the length in bits, little-endian.
	len <<= 3
	for i := uint(0); i < 8; i++ {
		tmp[i] = byte((len >> (8 * i)) & 0xff)
	}
	_, _ = d.Write(tmp[0:8])

	if d.nx != 0 {
		panic("md4: internal error: data left in buffer after padding")
	}

	var out [Size]byte
	for i, s := range d.s {
		out[i*4] = byte(s & 0xff)
		out[i*4+1] = byte((s >> 8) & 0xff)
		out[i*4+2] = byte((s >> 16) & 0xff)
		out[i*4+3] = byte((s >> 24) & 0xff)
	}
	return out
}

// Sum returns the MD4 checksum of data.
func Sum(data []byte) [Size]byte {
	var d digest
	d.Reset()
	_, _ = d.Write(data) // digest.Write never returns an error
	return d.checkSum()
}

// ContentHash returns the lowercase-hexadecimal MD4 digest of data, the form
// the Synology Drive wire protocol uses to identify file contents.
func ContentHash(data []byte) string {
	sum := Sum(data)
	return hex.EncodeToString(sum[:])
}
