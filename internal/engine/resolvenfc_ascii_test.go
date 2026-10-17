package engine

// The NFD fallback in resolveNFCComponents listed a component's parent
// directory on every miss of the literal (NFC) name - i.e. on every inbound create, since a
// node being created never exists yet. These tests pin the short-circuit that skips the
// listing for a component canonically equivalent only to itself, the exhaustiveness of the
// three-character exception set it rests on, and the unchanged behaviour of the real fallback.

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"

	"github.com/mfilippov/synosync/internal/normpath"
)

// countingReadDir wraps os.ReadDir with a call counter and records the directories listed.
type countingReadDir struct {
	calls int
	dirs  []string
}

func (c *countingReadDir) fn(name string) ([]os.DirEntry, error) {
	c.calls++
	c.dirs = append(c.dirs, name)
	return os.ReadDir(name)
}

// nfcASCIIEngine returns an Engine rooted at a fresh temp dir holding one view directory, with
// the readDir seam counting the NFD-fallback listings, plus the view directory's path.
func nfcASCIIEngine(t *testing.T) (*Engine, *countingReadDir, string) {
	t.Helper()
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.Mkdir(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view: %v", err)
	}
	c := &countingReadDir{}
	return &Engine{syncRoot: root, readDir: c.fn}, c, viewDir
}

// TestNFCASCIISingletonsAreExhaustive re-derives, over the whole Unicode code space, the set of
// ASCII characters that some NON-ASCII code point normalizes onto. asciiSelfEquivalent's
// short-circuit is only sound while that set is exactly nfcAmbiguousASCII, so an x/text update
// introducing a fourth such singleton must fail here rather than silently skip a legitimate
// NFD fallback.
func TestNFCASCIISingletonsAreExhaustive(t *testing.T) {
	var got []byte
	seen := map[byte]bool{}
	for r := rune(0x80); r <= 0x10FFFF; r++ {
		if r >= 0xD800 && r <= 0xDFFF { // surrogates are not scalar values
			continue
		}
		s := norm.NFC.String(string(r))
		if s == "" || !isASCII(s) {
			continue
		}
		for i := 0; i < len(s); i++ {
			if !seen[s[i]] {
				seen[s[i]] = true
				got = append(got, s[i])
			}
		}
	}
	if want := nfcAmbiguousASCII; !sameChars(string(got), want) {
		t.Fatalf("ASCII characters reachable by NFC from non-ASCII input = %q, want %q "+
			"(update nfcAmbiguousASCII and asciiSelfEquivalent's reasoning)", got, want)
	}
}

// isASCII reports whether s holds only ASCII bytes.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// sameChars reports whether a and b hold the same character set, order-insensitively.
func sameChars(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if !strings.ContainsRune(b, rune(a[i])) {
			return false
		}
	}
	return true
}

// TestAsciiSelfEquivalent pins the predicate itself, including the three conservative
// exceptions that keep the listing fallback.
func TestAsciiSelfEquivalent(t *testing.T) {
	tests := []struct {
		name string
		comp string
		want bool
	}{
		{"plain ascii", "report.txt", true},
		{"empty", "", true},
		{"punctuation and digits", "a-b_c.1 (2)!.tar.gz", true},
		{"uppercase K is the kelvin sign's NFC form", "Kernel.log", false},
		{"lowercase k is unambiguous", "kernel.log", true},
		{"semicolon is the greek question mark's NFC form", "a;b.txt", false},
		{"backtick is the greek varia's NFC form", "a`b.txt", false},
		{"precomposed non-ascii", nfcName, false},
		{"decomposed non-ascii", nfdName, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := asciiSelfEquivalent(tt.comp); got != tt.want {
				t.Errorf("asciiSelfEquivalent(%q) = %v, want %v", tt.comp, got, tt.want)
			}
		})
	}
}

// TestResolveNFCComponentsASCIISkipsListing is the regression guard: resolving a not-yet-
// existing ASCII leaf - what every inbound create does, through onDiskAbsPrefix - must not
// list the parent directory, because no on-disk spelling other than the literal one can
// normalize to an ASCII name. Both the materialising (prefix) and the strict resolver are
// covered, as is an ASCII component holding one of the three ambiguous characters, which
// deliberately keeps the listing.
func TestResolveNFCComponentsASCIISkipsListing(t *testing.T) {
	t.Run("prefix resolver keeps a fresh ascii leaf literal without listing", func(t *testing.T) {
		e, c, _ := nfcASCIIEngine(t)
		rel := filepath.Join("Photos", "new.txt")
		got, ok := e.resolveNFCPathPrefix(rel)
		if !ok || got != rel {
			t.Fatalf("resolveNFCPathPrefix(%q) = (%q, %v), want (%q, true)", rel, got, ok, rel)
		}
		if c.calls != 0 {
			t.Errorf("listed %d directories (%v), want 0 for an ASCII component", c.calls, c.dirs)
		}
	})

	t.Run("strict resolver reports an absent ascii leaf without listing", func(t *testing.T) {
		e, c, _ := nfcASCIIEngine(t)
		if got, ok := e.resolveNFCPath(filepath.Join("Photos", "gone.txt")); ok {
			t.Fatalf("resolveNFCPath matched a nonexistent file: %q", got)
		}
		if c.calls != 0 {
			t.Errorf("listed %d directories (%v), want 0 for an ASCII component", c.calls, c.dirs)
		}
	})

	t.Run("existing ascii components never list", func(t *testing.T) {
		e, c, viewDir := nfcASCIIEngine(t)
		if err := os.MkdirAll(filepath.Join(viewDir, "a", "b"), 0o755); err != nil {
			t.Fatalf("seed dirs: %v", err)
		}
		if err := os.WriteFile(filepath.Join(viewDir, "a", "b", "f.txt"), []byte("x"), 0o644); err != nil {
			t.Fatalf("seed file: %v", err)
		}
		rel := filepath.Join("Photos", "a", "b", "f.txt")
		if got, ok := e.resolveNFCPath(rel); !ok || got != rel {
			t.Fatalf("resolveNFCPath(%q) = (%q, %v), want (%q, true)", rel, got, ok, rel)
		}
		if c.calls != 0 {
			t.Errorf("listed %d directories (%v), want 0", c.calls, c.dirs)
		}
	})

	t.Run("ambiguous ascii component still lists", func(t *testing.T) {
		e, c, _ := nfcASCIIEngine(t)
		// "K" is the NFC form of U+212A KELVIN SIGN, so an on-disk name spelled with the
		// Kelvin sign would normalize onto this component: the fallback must still run.
		if _, ok := e.resolveNFCPath(filepath.Join("Photos", "K.txt")); ok {
			t.Fatal("resolveNFCPath matched a nonexistent file")
		}
		if c.calls == 0 {
			t.Error("skipped the listing for a component holding an NFC-ambiguous ASCII character")
		}
	})
}

// TestResolveNFCComponentsNFDFallbackUnchanged confirms the short-circuit did not narrow the
// real fallback: a file stored on disk under its decomposed (NFD) bytes is still resolved from
// the canonical NFC path the store and the queued op carry, for both resolvers and at both a
// directory and a leaf component.
func TestResolveNFCComponentsNFDFallbackUnchanged(t *testing.T) {
	if nfcName == nfdName || normpath.Normalize(nfdName) != nfcName {
		t.Fatalf("test setup: NFC %q and NFD %q must differ but normalize equal", nfcName, nfdName)
	}
	e, c, viewDir := nfcASCIIEngine(t)
	if err := os.Mkdir(filepath.Join(viewDir, nfdDir), 0o755); err != nil {
		t.Fatalf("seed NFD dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, nfdDir, nfdName), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed NFD file: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, nfdDir, nfcName)); err == nil {
		t.Skip("filesystem normalizes names; the NFD fallback cannot be exercised here")
	}

	nfcRel := filepath.Join("Photos", nfcDir, nfcName)
	wantRel := filepath.Join("Photos", nfdDir, nfdName)
	got, ok := e.resolveNFCPath(nfcRel)
	if !ok || got != wantRel {
		t.Fatalf("resolveNFCPath(%q) = (%q, %v), want (%q, true)", nfcRel, got, ok, wantRel)
	}
	if c.calls == 0 {
		t.Error("resolved an NFD name without listing: the fallback did not run")
	}

	// The materialising resolver reuses the existing NFD directory and keeps a fresh ASCII
	// leaf under it literal - the inbound-create shape.
	freshRel := filepath.Join("Photos", nfcDir, "new.txt")
	wantFresh := filepath.Join("Photos", nfdDir, "new.txt")
	if got, ok := e.resolveNFCPathPrefix(freshRel); !ok || got != wantFresh {
		t.Fatalf("resolveNFCPathPrefix(%q) = (%q, %v), want (%q, true)", freshRel, got, ok, wantFresh)
	}
}

// benchFlatDir materialises a flat view directory of n ASCII-named files and returns an Engine
// rooted above it. It models the cold-start hot path: a large flat directory into which
// inbound creates land one after another.
func benchFlatDir(b *testing.B, n int) *Engine {
	b.Helper()
	root := b.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.Mkdir(viewDir, 0o755); err != nil {
		b.Fatalf("mkdir view: %v", err)
	}
	for i := 0; i < n; i++ {
		f := filepath.Join(viewDir, "file"+strconv.Itoa(i)+".bin")
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			b.Fatalf("seed file: %v", err)
		}
	}
	return &Engine{syncRoot: root, readDir: os.ReadDir}
}

// BenchmarkResolveNFCPathPrefixFlatDir measures resolving a not-yet-existing leaf in a flat
// 5k-entry directory. "ascii" is the short-circuit path (Lstat only); "nonascii" keeps the
// listing fallback and therefore reproduces the pre-short-circuit cost of every inbound create.
func BenchmarkResolveNFCPathPrefixFlatDir(b *testing.B) {
	const entries = 5000
	e := benchFlatDir(b, entries)
	b.Run("ascii", func(b *testing.B) {
		rel := filepath.Join("Photos", "fresh.bin")
		for i := 0; i < b.N; i++ {
			if _, ok := e.resolveNFCPathPrefix(rel); !ok {
				b.Fatal("resolveNFCPathPrefix failed")
			}
		}
	})
	b.Run("nonascii", func(b *testing.B) {
		rel := filepath.Join("Photos", nfcName)
		for i := 0; i < b.N; i++ {
			if _, ok := e.resolveNFCPathPrefix(rel); !ok {
				b.Fatal("resolveNFCPathPrefix failed")
			}
		}
	})
}
