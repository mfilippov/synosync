package normpath

import (
	"runtime"
	"sync"
	"testing"
)

// NFD (decomposed) and NFC (precomposed) encodings of the same user-visible
// names, written with explicit escapes so the source stays in the intended
// Unicode form.
const (
	nfcE  = "café"    // café - precomposed é (U+00E9)
	nfdE  = "café"   // café - e + combining acute (U+0065 U+0301)
	nfcU  = "Müller"  // Müller - precomposed ü (U+00FC)
	nfdU  = "Müller" // Müller - u + combining diaeresis (U+0075 U+0308)
	upNFC = "CAFÉ"    // CAFÉ - precomposed, upper-case
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"already NFC é", nfcE, nfcE},
		{"NFD é → NFC", nfdE, nfcE},
		{"already NFC ü", nfcU, nfcU},
		{"NFD ü → NFC", nfdU, nfcU},
		{"ASCII unchanged", "foo/Bar.txt", "foo/Bar.txt"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Normalize(tt.in); got != tt.want {
				t.Errorf("Normalize(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeNFDDiffersFromNFC(t *testing.T) {
	// Guard the test fixtures themselves: the NFD literals must be genuinely
	// decomposed (longer byte length) so the NFD→NFC assertions are meaningful.
	if nfdE == nfcE || len(nfdE) <= len(nfcE) {
		t.Fatalf("nfdE %q is not decomposed relative to nfcE %q", nfdE, nfcE)
	}
	if nfdU == nfcU || len(nfdU) <= len(nfcU) {
		t.Fatalf("nfdU %q is not decomposed relative to nfcU %q", nfdU, nfcU)
	}
}

func TestNormalizeIdempotent(t *testing.T) {
	for _, in := range []string{nfcE, nfdE, nfcU, nfdU, "foo/Bar.txt", ""} {
		once := Normalize(in)
		if twice := Normalize(once); twice != once {
			t.Errorf("Normalize not idempotent for %q: %q then %q", in, once, twice)
		}
	}
}

func TestFoldCaseInsensitive(t *testing.T) {
	defer SetCaseInsensitive(true)()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"upper → lower", "Foo.txt", "foo.txt"},
		{"mixed → lower", "FooBar/Baz.TXT", "foobar/baz.txt"},
		{"already lower", "foo.txt", "foo.txt"},
		{"NFD é lower-cased to NFC lower", nfdE, nfcE},
		{"NFC é stays", nfcE, nfcE},
		{"upper NFC É → lower NFC é", upNFC, nfcE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Fold(tt.in); got != tt.want {
				t.Errorf("Fold(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestFoldCaseSensitive(t *testing.T) {
	defer SetCaseInsensitive(false)()

	// On a case-sensitive platform Fold is the identity over NFC: case is
	// preserved, only Unicode form is folded.
	if got := Fold("Foo.txt"); got != "Foo.txt" {
		t.Errorf("Fold(Foo.txt) on case-sensitive = %q, want Foo.txt", got)
	}
	if got := Fold(nfdE); got != nfcE {
		t.Errorf("Fold(NFD é) on case-sensitive = %q, want %q (NFC, case preserved)", got, nfcE)
	}
}

func TestFoldCaseVariantsMatch(t *testing.T) {
	// foo vs Foo: same key only when case-insensitive.
	t.Run("case-insensitive", func(t *testing.T) {
		defer SetCaseInsensitive(true)()
		if Fold("foo.txt") != Fold("Foo.txt") {
			t.Error("case-insensitive: foo.txt and Foo.txt should fold to the same key")
		}
	})
	t.Run("case-sensitive", func(t *testing.T) {
		defer SetCaseInsensitive(false)()
		if Fold("foo.txt") == Fold("Foo.txt") {
			t.Error("case-sensitive: foo.txt and Foo.txt should NOT fold to the same key")
		}
	})
}

func TestKey(t *testing.T) {
	defer SetCaseInsensitive(true)()
	// Key == Fold(Normalize(p)); NFD-vs-NFC and case variants all collapse.
	if Key(nfdE) != Key(upNFC) {
		t.Errorf("Key(%q)=%q should equal Key(%q)=%q", nfdE, Key(nfdE), upNFC, Key(upNFC))
	}
}

// TestFoldConcurrentWithSetCaseInsensitive exercises the documented seam
// contract: SetCaseInsensitive may flip the flag while other goroutines are
// calling Fold on hot paths (engine goroutines still live during a test's
// deferred restore). With the pre-atomic plain-bool flag this test fails under
// -race with a data-race report.
func TestFoldConcurrentWithSetCaseInsensitive(t *testing.T) {
	defer SetCaseInsensitive(defaultCaseInsensitive("linux"))()

	const iterations = 1000
	var wg sync.WaitGroup
	start := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			restore := SetCaseInsensitive(i%2 == 0)
			restore()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < iterations; i++ {
			// Either the folded or unfolded form is acceptable mid-toggle;
			// the assertion is only that the read is race-free and returns
			// one of the two coherent values.
			got := Fold("Foo.txt")
			if got != "foo.txt" && got != "Foo.txt" {
				t.Errorf("Fold(Foo.txt) mid-toggle = %q, want foo.txt or Foo.txt", got)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}

// TestFoldApproximationEdgeCases pins the strings.ToLower approximation
// documented on Fold: the known divergences from the NTFS upcase table and
// APFS full folding. A change in these outcomes (e.g. a Unicode table update
// or a switch to full case folding) must be a conscious decision - it shifts
// which names hit the scanKeyCollisions guard vs. the spurious delete+create
// degradation (DESIGN §13).
func TestFoldApproximationEdgeCases(t *testing.T) {
	defer SetCaseInsensitive(true)()

	// U+0130 İ collides with plain I/i (false collision vs. NTFS, which
	// upcases İ to itself; caught by the scanKeyCollisions guard).
	if Fold("İ") != Fold("I") || Fold("İ") != "i" {
		t.Errorf("Fold(İ) = %q, want %q (collides with I/i)", Fold("İ"), "i")
	}

	// U+1E9E ẞ lowers to U+00DF ß and stays distinct from ss/SS (false
	// distinction vs. APFS full folding, which folds both to "ss").
	if Fold("ẞ") != "ß" {
		t.Errorf("Fold(ẞ) = %q, want ß", Fold("ẞ"))
	}
	if Fold("ẞ") == Fold("SS") {
		t.Error("Fold(ẞ) should stay distinct from Fold(SS) under the ToLower approximation")
	}

	// Cherokee case pairs (Unicode 8.0) collide here, unlike NTFS upcase
	// tables frozen before those mappings existed.
	if Fold("Ꭰ") != Fold("ꭰ") {
		t.Errorf("Fold(Ꭰ)=%q and Fold(ꭰ)=%q should collide under ToLower", Fold("Ꭰ"), Fold("ꭰ"))
	}
}

func TestDefaultCaseInsensitive(t *testing.T) {
	tests := map[string]bool{
		"windows": true,
		"darwin":  true,
		"linux":   false,
		"freebsd": false,
	}
	for goos, want := range tests {
		if got := defaultCaseInsensitive(goos); got != want {
			t.Errorf("defaultCaseInsensitive(%q) = %v, want %v", goos, got, want)
		}
	}
}

func TestExportedDefaultCaseInsensitive(t *testing.T) {
	if got := DefaultCaseInsensitive(); got != defaultCaseInsensitive(runtime.GOOS) {
		t.Errorf("DefaultCaseInsensitive() = %v, want GOOS default", got)
	}
}

// TestUseCaseInsensitiveOverridesFlag exercises the production one-way setter the
// engine's sync-root probe drives: UseCaseInsensitive replaces the process-wide
// flag (visible through CaseInsensitive and Fold) without a restore func, unlike
// the test-only SetCaseInsensitive. The original value is restored on exit.
func TestUseCaseInsensitiveOverridesFlag(t *testing.T) {
	orig := CaseInsensitive()
	defer UseCaseInsensitive(orig)

	UseCaseInsensitive(true)
	if !CaseInsensitive() {
		t.Fatal("CaseInsensitive() = false after UseCaseInsensitive(true)")
	}
	if got := Fold("Foo.txt"); got != "foo.txt" {
		t.Errorf("Fold(Foo.txt) = %q after UseCaseInsensitive(true), want foo.txt", got)
	}

	UseCaseInsensitive(false)
	if CaseInsensitive() {
		t.Fatal("CaseInsensitive() = true after UseCaseInsensitive(false)")
	}
	if got := Fold("Foo.txt"); got != "Foo.txt" {
		t.Errorf("Fold(Foo.txt) = %q after UseCaseInsensitive(false), want Foo.txt", got)
	}
}

// TestNormalizeCollapsesSingletons pins the third, previously undocumented source of
// key collisions: Unicode SINGLETON decompositions. NFC does not merely recombine
// base+combining sequences - it also maps a handful of compatibility duplicates onto
// their canonical twin, and the twin can be a completely different character:
// U+212A KELVIN SIGN becomes ASCII "K", U+2126 OHM SIGN becomes U+03A9 GREEK CAPITAL
// OMEGA, U+212B ANGSTROM SIGN becomes U+00C5.
//
// So Normalize is not injective, and the collision it creates is NOT the NFD/NFC pair
// every other comment in this tree describes, and NOT case-dependent. On a
// case-SENSITIVE Linux filesystem, "K.txt" spelled with U+212A and "K.txt" spelled with
// ASCII K are two distinct files that the store would key identically - and one of the
// two names looks like plain ASCII, so nothing about it reads as a Unicode hazard. The
// engine's collision guards catch it because they key on normpath.Key rather than on
// anything NFD-specific (scanKeyCollisions locally, catalogKeyCollisions on the server
// catalog): both spellings are left protected and unsynced instead of one silently
// overwriting the other. This test exists so a future "optimisation" that special-cases
// the guards to NFD input has to break it.
func TestNormalizeCollapsesSingletons(t *testing.T) {
	restore := SetCaseInsensitive(false) // case-sensitive: the fold is not what merges these
	defer restore()

	cases := []struct {
		name      string
		singleton string
		canonical string
	}{
		{"kelvin_sign_to_ascii_K", "K", "K"},
		{"ohm_sign_to_greek_omega", "Ω", "Ω"},
		{"angstrom_sign_to_a_ring", "Å", "Å"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.singleton == tc.canonical {
				t.Fatalf("test bug: %+q and %+q are the same string", tc.singleton, tc.canonical)
			}
			if got := Normalize(tc.singleton); got != tc.canonical {
				t.Fatalf("Normalize(%+q) = %+q, want %+q", tc.singleton, got, tc.canonical)
			}
			// The consequence the guards must handle: two distinct on-disk names, one key.
			a, b := Key(tc.singleton+".txt"), Key(tc.canonical+".txt")
			if a != b {
				t.Fatalf("Key(%+q) = %+q and Key(%+q) = %+q; want the collision these names really have",
					tc.singleton+".txt", a, tc.canonical+".txt", b)
			}
		})
	}
}
