// MD4 block step. The output of this function is the new digest state, derived
// from the current state and a sequence of one or more 64-byte blocks, as
// described in RFC 1320, section 3.4.
package md4

import "math/bits"

// shift amounts for the three rounds.
var shift1 = []int{3, 7, 11, 19}
var shift2 = []int{3, 5, 9, 13}
var shift3 = []int{3, 9, 11, 15}

// permutations of the 16 message words for rounds 2 and 3.
var xIndex2 = []uint{0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15}
var xIndex3 = []uint{0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15}

func block(dig *digest, p []byte) {
	a := dig.s[0]
	b := dig.s[1]
	c := dig.s[2]
	d := dig.s[3]
	var X [16]uint32
	for len(p) >= BlockSize {
		aa, bb, cc, dd := a, b, c, d

		j := 0
		for i := 0; i < 16; i++ {
			X[i] = uint32(p[j]) | uint32(p[j+1])<<8 | uint32(p[j+2])<<16 | uint32(p[j+3])<<24
			j += 4
		}

		// Round 1.
		for i := uint(0); i < 16; i++ {
			x := i
			s := shift1[i%4]
			f := ((c ^ d) & b) ^ d
			a += f + X[x]
			a = bits.RotateLeft32(a, s)
			a, b, c, d = d, a, b, c
		}

		// Round 2.
		for i := uint(0); i < 16; i++ {
			x := xIndex2[i]
			s := shift2[i%4]
			g := (b & c) | (b & d) | (c & d)
			a += g + X[x] + 0x5a827999
			a = bits.RotateLeft32(a, s)
			a, b, c, d = d, a, b, c
		}

		// Round 3.
		for i := uint(0); i < 16; i++ {
			x := xIndex3[i]
			s := shift3[i%4]
			h := b ^ c ^ d
			a += h + X[x] + 0x6ed9eba1
			a = bits.RotateLeft32(a, s)
			a, b, c, d = d, a, b, c
		}

		a += aa
		b += bb
		c += cc
		d += dd

		p = p[BlockSize:]
	}

	dig.s[0] = a
	dig.s[1] = b
	dig.s[2] = c
	dig.s[3] = d
}
