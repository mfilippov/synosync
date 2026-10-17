package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

func TestConflictCopyName(t *testing.T) {
	// A fixed instant so the embedded stamp is deterministic: Jun 11 2026 15:30:04.
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	const stamp = "Jun-11-153004-2026"

	cases := []struct {
		name    string
		relPath string
		want    string
	}{
		{
			name:    "extension",
			relPath: "report.txt",
			want:    "report_laptop_" + stamp + "_Conflict.txt",
		},
		{
			name:    "no extension",
			relPath: "README",
			want:    "README_laptop_" + stamp + "_Conflict",
		},
		{
			name:    "nested path",
			relPath: "docs/sub/report.txt",
			want:    "docs/sub/report_laptop_" + stamp + "_Conflict.txt",
		},
		{
			name:    "dotfile",
			relPath: ".bashrc",
			want:    ".bashrc_laptop_" + stamp + "_Conflict",
		},
		{
			name:    "double extension",
			relPath: "archive.tar.gz",
			want:    "archive.tar_laptop_" + stamp + "_Conflict.gz",
		},
		{
			name:    "nested dotfile",
			relPath: "home/user/.vimrc",
			want:    "home/user/.vimrc_laptop_" + stamp + "_Conflict",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := conflictCopyName(tc.relPath, host, ts)
			if got != tc.want {
				t.Fatalf("conflictCopyName(%q) = %q, want %q", tc.relPath, got, tc.want)
			}
		})
	}
}

// TestConflictCopyNameTruncatesLongName verifies a base name long enough that appending the
// host/timestamp/_Conflict marker would exceed the filesystem per-component limit is truncated
// so the on-disk basename fits - otherwise RenameNoReplace fails forever with ENAMETOOLONG and
// wedges the resolver. The marker and extension that identify the copy survive.
func TestConflictCopyNameTruncatesLongName(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	longName := "dir/" + strings.Repeat("a", 300) + ".txt"

	got := conflictCopyName(longName, host, ts)
	base := path.Base(got)
	if len(base) > maxCopyNameComponentBytes {
		t.Fatalf("copy basename = %d bytes, want <= %d", len(base), maxCopyNameComponentBytes)
	}
	if dir := path.Dir(got); dir != "dir" {
		t.Errorf("directory portion = %q, want %q", dir, "dir")
	}
	wantSuffix := "_" + host + "_" + ts.Format(conflictTimeLayout) + "_Conflict.txt"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("copy name %q lost its marker/extension suffix %q", got, wantSuffix)
	}
}

// TestConflictCopyNameTruncatesLongExtension verifies the basename stays within the component
// limit even when a pathologically long extension (not the base name) would push it over - the
// extension is trimmed too, otherwise RenameNoReplace would fail forever with ENAMETOOLONG and
// wedge the resolver.
func TestConflictCopyNameTruncatesLongExtension(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	longExt := "f." + strings.Repeat("e", 400)

	got := conflictCopyName(longExt, host, ts)
	if base := path.Base(got); len(base) > maxCopyNameComponentBytes {
		t.Fatalf("copy basename = %d bytes, want <= %d", len(base), maxCopyNameComponentBytes)
	}
}

// TestConflictCopyNameTruncatesOnRuneBoundary verifies truncation never splits a multibyte
// UTF-8 rune: a name of three-byte runes plus a long host forces a cut whose byte
// budget (255-marker-ext) lands mid-rune, so the backtrack to a rune boundary is genuinely
// exercised - a two-byte rune would, for an even budget, land on a boundary by luck and never
// run the backtrack. The result must remain valid UTF-8 and within the component limit.
func TestConflictCopyNameTruncatesOnRuneBoundary(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	host := strings.Repeat("h", 100)
	name := strings.Repeat("€", 200) + ".txt" // "€" is three bytes in UTF-8.

	got := conflictCopyName(name, host, ts)
	if !utf8.ValidString(got) {
		t.Fatalf("truncation split a multibyte rune: %q is not valid UTF-8", got)
	}
	if len(path.Base(got)) > maxCopyNameComponentBytes {
		t.Fatalf("copy basename = %d bytes, want <= %d", len(path.Base(got)), maxCopyNameComponentBytes)
	}
}

// TestConflictCopyNameCounterKeepsMarkerAndLengthBound verifies the collision-counter variant
// stays within the per-component byte limit AND keeps the "_Conflict" marker whole.
// conflictCopyName truncates the basename to exactly maxCopyNameComponentBytes, so a naive
// "_<n>" insertion would push it over and wedge RenameNoReplace forever with ENAMETOOLONG -
// the exact failure the truncation guards against. Re-trimming the ASSEMBLED name to make room
// (the pre-v15 withCopyCounter) stays inside the limit but eats the marker's tail instead: the
// name stops matching DESIGN §2's contract and watcher/filter.go's isConflictCopyName, so the
// watcher re-reports our own keep-both copy as user activity. Budgeting the counter WITH the
// marker (conflictCopyNameN) satisfies both.
func TestConflictCopyNameCounterKeepsMarkerAndLengthBound(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	// A name long enough that conflictCopyName truncates the basename to the cap.
	longName := "dir/" + strings.Repeat("a", 300) + ".txt"
	base := conflictCopyName(longName, host, ts)
	if len(path.Base(base)) != maxCopyNameComponentBytes {
		t.Fatalf("precondition: conflictCopyName basename = %d bytes, want exactly %d (test needs an at-limit name)",
			len(path.Base(base)), maxCopyNameComponentBytes)
	}

	for _, n := range []int{1, 9, 99, 9999} {
		got := conflictCopyNameN(longName, host, ts, n)
		gotBase := path.Base(got)
		if len(gotBase) > maxCopyNameComponentBytes {
			t.Errorf("conflictCopyNameN(_, %d) basename = %d bytes, want <= %d", n, len(gotBase), maxCopyNameComponentBytes)
		}
		if !utf8.ValidString(got) {
			t.Errorf("conflictCopyNameN(_, %d) = %q is not valid UTF-8", n, got)
		}
		wantSuffix := "_Conflict_" + strconv.Itoa(n) + ".txt"
		if !strings.HasSuffix(got, wantSuffix) {
			t.Errorf("conflictCopyNameN(_, %d) = %q lost the marker/counter/extension suffix %q", n, got, wantSuffix)
		}
		// Mirror watcher/filter.go isConflictCopyName: strip the extension, then a trailing
		// "_<digits>" counter, and the remainder must end in the marker.
		name := strings.TrimSuffix(gotBase, ".txt")
		if i := strings.LastIndex(name, "_"); i > 0 && name[i+1:] == strconv.Itoa(n) {
			name = name[:i]
		}
		if !strings.HasSuffix(name, "_Conflict") {
			t.Errorf("conflictCopyNameN(_, %d) = %q is not recognisable as a keep-both copy", n, got)
		}
		if dir := path.Dir(got); dir != "dir" {
			t.Errorf("conflictCopyNameN(_, %d) directory portion = %q, want %q", n, dir, "dir")
		}
	}
}

// TestTruncateUTF8 exercises the helper directly: the fits-as-is early return and the
// rune-boundary backtrack, the two branches the conflictCopyName-driven tests only reach
// indirectly (and, for two-byte runes on an even budget, not at all).
func TestTruncateUTF8(t *testing.T) {
	tests := []struct {
		name string
		s    string
		n    int
		want string
	}{
		{"fits exactly", "abc", 3, "abc"},
		{"fits with room", "abc", 10, "abc"},
		{"empty", "", 5, ""},
		{"zero budget", "abc", 0, ""},
		{"ascii cut on boundary", "abcdef", 3, "abc"},
		// "€" is three bytes (0xE2 0x82 0xAC). A budget of 4 lands inside the second
		// rune, so the backtrack must drop back to the boundary after the first rune.
		{"three-byte rune backtrack", "€€€", 4, "€"},
		{"three-byte rune mid-second", "€€€", 5, "€"},
		{"three-byte rune on boundary", "€€€", 6, "€€"},
		// "é" is two bytes (0xC3 0xA9). An odd budget lands on a continuation byte.
		{"two-byte rune backtrack", "ééé", 3, "é"},
		{"two-byte rune on boundary", "ééé", 4, "éé"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateUTF8(tt.s, tt.n)
			if got != tt.want {
				t.Errorf("truncateUTF8(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
			}
			if !utf8.ValidString(got) {
				t.Errorf("truncateUTF8(%q, %d) = %q is not valid UTF-8", tt.s, tt.n, got)
			}
			if len(got) > tt.n {
				t.Errorf("truncateUTF8(%q, %d) = %q is %d bytes, want <= %d", tt.s, tt.n, got, len(got), tt.n)
			}
		})
	}
}

// pinClock fixes the engine's clock and hostname so a resolver's conflict-copy name and
// detected-at timestamp are deterministic.
func pinClock(e *Engine, host string, ts time.Time) {
	e.hostname = host
	e.now = func() time.Time { return ts }
}

// TestDiskFullLatchesOnConflictResolve confirms an ENOSPC failure on the keep-both RESOLVE
// path - which applyOp routes through an early return ahead of the inbound switch's old latch
// site - still latches DiskFull() and leaves the op queued (not dead-lettered), now that the
// latch is centralised in noteApplyResult. The ENOSPC is injected at the first server
// turn (QueryNode) so no disk mutation happens before the failure; clearing it and re-draining
// completes the resolution and clears the latch.
func TestDiskFullLatchesOnConflictResolve(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
		// The first server turn (QueryNode) hits a disk-full condition before any local
		// disk mutation, so a retry after freeing space resolves cleanly.
		queryNodeErr: fmt.Errorf("commit: %w", syscall.ENOSPC),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the ENOSPC resolve apply error")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the ENOSPC resolve op still queued (1)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen = %d (err %v), want ENOSPC NOT dead-lettered (0)", n, err)
	}
	if !e.DiskFull() {
		t.Fatal("DiskFull() false after an ENOSPC resolve apply, want it latched")
	}

	// Free the "disk": the next drain resolves the conflict and clears the flag.
	c.mu.Lock()
	c.queryNodeErr = nil
	c.mu.Unlock()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue after freeing space: %v", err)
	}
	if e.DiskFull() {
		t.Error("DiskFull() still latched after a successful resolve apply, want it cleared")
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q after retry", got, err, serverBytes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained after retry: %d", n)
	}
}

func TestResolveConflictKeepBoth(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path now holds the server bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The _Conflict copy holds the old local bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// The copy was uploaded.
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
	if u := c.uploads[0]; u.parentID != "root1" || u.name != copyName || string(u.content) != string(localBytes) {
		t.Errorf("upload call = %+v, want parent root1 name %q content %q", u, copyName, localBytes)
	}
	// Both baselines are synced.
	cb, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" || cb.ServerHash != serverHash || cb.LocalHash != serverHash {
		t.Errorf("canonical baseline = %+v, want synced srv1/%s", cb, serverHash)
	}
	pb, err := st.GetBaseline(7, copyRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if pb.State != store.StateSynced || pb.FileID != "copyf" || pb.LocalHash != localHash {
		t.Errorf("copy baseline = %+v, want synced copyf/%s", pb, localHash)
	}
	// A durable conflict record exists.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("conflict records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.CanonicalPath != "a.txt" || r.CopyPath != copyRel || r.CopyFileID != "copyf" || r.DetectedAt != ts.Unix() {
		t.Errorf("conflict record = %+v, want a.txt/%s/copyf @ %d", r, copyRel, ts.Unix())
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictPreservesConcurrentWrite models a fresh local file landing at the
// canonical path mid-resolution - after the divergent local content was set aside but before
// the server version is promoted. The no-replace promotion must refuse to clobber it; the
// resolver sets that intruder aside as its own conflict copy and retries, so neither the
// original divergent content nor the concurrent edit is lost.
func TestResolveConflictPreservesConcurrentWrite(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	intruderBytes := []byte("fresh-concurrent-edit")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// The download fires once; use it to drop a fresh local file at the canonical path exactly
	// when the server version is being fetched (after the divergent content was set aside).
	c.downloadHook = func() {
		if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), intruderBytes, 0o644); err != nil {
			t.Errorf("seed concurrent intruder: %v", err)
		}
	}

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Canonical holds the server bytes (promoted after the intruder was set aside).
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The original divergent local content is the base conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || !bytes.Equal(got, localBytes) {
		t.Fatalf("base copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// The concurrent edit was NOT clobbered: it was set aside as the next counter-suffixed copy.
	intruderRel := conflictCopyNameN("a.txt", host, ts, 1)
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.Base(intruderRel))); err != nil || !bytes.Equal(got, intruderBytes) {
		t.Fatalf("concurrent edit lost: %q (err %v), want %q preserved at %q", got, err, intruderBytes, intruderRel)
	}
	// Only the original divergent content is pushed back as a copy; the set-aside intruder is
	// left on disk for the next local scan to surface, not re-uploaded by the resolver.
	if len(c.uploads) != 1 || !bytes.Equal(c.uploads[0].content, localBytes) {
		t.Fatalf("uploads = %+v, want exactly one upload of the original divergent content %q", c.uploads, localBytes)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictIntruderProvenanceRecorded verifies that a fresh local file
// set aside during downloadCanonical's no-replace retry gets a durable Conflict record at
// set-aside time (preserveAndRecord), not a bare preserveConflictCopy that parks content with
// no provenance. The copy push is failed so the resolution stops right after the retry -
// modelling the crash window - and the surviving record must point at the intruder's copy,
// the most recently parked content.
func TestResolveConflictIntruderProvenanceRecorded(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	intruderBytes := []byte("fresh-concurrent-edit")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	intruderRel := conflictCopyNameN("a.txt", host, ts, 1)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		// The copy push fails, aborting the resolution after the intruder was set aside.
		uploadErr: errors.New("simulated copy-push failure"),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	c.downloadHook = func() {
		if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), intruderBytes, 0o644); err != nil {
			t.Errorf("seed concurrent intruder: %v", err)
		}
	}

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want copy-push failure, got nil")
	}

	// Both parked copies exist on disk, and the canonical path holds the promoted server bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.Base(copyRel))); err != nil || !bytes.Equal(got, localBytes) {
		t.Fatalf("base copy content = %q (err %v), want %q", got, err, localBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.Base(intruderRel))); err != nil || !bytes.Equal(got, intruderBytes) {
		t.Fatalf("intruder copy content = %q (err %v), want %q", got, err, intruderBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The durable provenance record names the intruder's copy - the record was written when
	// the intruder was set aside, so a crash in this window never leaves parked content
	// missing from the Phase-8 conflicts list.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != intruderRel {
		t.Fatalf("conflict records = %+v, want one for a.txt -> %s (the intruder's copy)", recs, intruderRel)
	}
}

// TestResolveConflictProvenancePersistedBeforeDownload verifies that the durable Conflict
// provenance is written keyed to the copy path BEFORE the (non-transactional) download, so a
// failure (modelling a crash) after the divergent file is set aside but before the resolution
// completes still leaves a record pointing at the preserved copy. The op stays queued to retry.
func TestResolveConflictProvenancePersistedBeforeDownload(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: "serverhash", MTime: 555, Size: 14},
		},
		// The download fails: the resolution aborts after the file was set aside.
		downloadErr: errors.New("simulated download failure"),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain fails (download error), leaving the op queued - but the side effects up to the
	// failure must have happened.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want download error, got nil")
	}
	// The divergent local content was set aside as the conflict copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || !bytes.Equal(got, localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q set aside", got, err, localBytes)
	}
	// The durable provenance record exists and points at that copy, even though the resolution
	// never completed - so a crash here would not lose the conflict from the Phase-8 list.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one for a.txt -> %s persisted before the download", recs, copyRel)
	}
}

// TestResolveConflictKeepBothLargeCopy exercises the resume (chunked) branch of
// uploadConflictCopy: the divergent local file is larger than uploadResumeThreshold, so
// the preserved _Conflict copy is pushed back via resume_upload rather than a single-frame
// Upload. It asserts the streamed bytes reach the server intact and the copy baseline
// records the streamed hash/size plus the resume reply's file id.
func TestResolveConflictKeepBothLargeCopy(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	serverHash := md4.ContentHash(serverBytes)
	// A divergent local file larger than the resume threshold forces the chunked copy push.
	localBytes := make([]byte, uploadResumeThreshold+1234)
	for i := range localBytes {
		localBytes[i] = byte(i % 251)
	}
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("big.bin", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"big.bin": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		resumeToken:     actions.ResumeToken{Token: "tk"},
		resumeNode:      actions.Node{FileID: "copyf", Hash: localHash, MTime: 777},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "big.bin", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path holds the server bytes; the copy holds the old local bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "big.bin")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || !bytes.Equal(got, localBytes) {
		t.Fatalf("copy content mismatch (err %v)", err)
	}
	// The chunked resume path (not single-frame Upload) carried the copy, and the streamed
	// bytes reassembled to exactly the local content.
	if len(c.uploads) != 0 {
		t.Errorf("single-frame Upload used for large copy: %d calls", len(c.uploads))
	}
	if !bytes.Equal(c.resumeAssembled, localBytes) {
		t.Errorf("resume-assembled copy bytes do not match local content (got %d bytes, want %d)",
			len(c.resumeAssembled), len(localBytes))
	}
	// The copy baseline records the streamed hash/size and the resume reply's file id.
	pb, err := st.GetBaseline(7, copyRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if pb.State != store.StateSynced || pb.FileID != "copyf" || pb.LocalHash != localHash || pb.LocalSize != int64(len(localBytes)) {
		t.Errorf("copy baseline = %+v, want synced copyf/%s size %d", pb, localHash, len(localBytes))
	}
	// Durable conflict record points at the resumed copy.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CopyFileID != "copyf" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one for %s/copyf", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictLargeServerNodeUsesResume exercises the resume (chunked) branch of the
// keep-both canonical download: the diverged SERVER node is larger than UploadResumeThreshold,
// so downloadCanonical must fetch it in offset-based resume_download chunks (mirroring the
// inbound path) rather than a single-frame Download - so a network drop resumes from the last
// offset instead of restarting at 0. It asserts the server bytes land on the canonical path via
// the resume loop (successive offsets, no single-frame Download) and the local copy survives.
func TestResolveConflictLargeServerNodeUsesResume(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	// A server node larger than the (overridden) resume threshold forces the chunked download.
	serverBytes := make([]byte, 200)
	for i := range serverBytes {
		serverBytes[i] = byte((i * 7) % 251)
	}
	serverHash := md4.ContentHash(serverBytes)
	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("big.bin", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"big.bin": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent:   map[string][]byte{"srv1": serverBytes},
		downloadNode:      map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		resumeDLChunkSize: 16, // server chooses 16-byte chunks
		// The preserved local copy is small, so its push back is a single-frame Upload.
		uploadNode: map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)
	e.limits.UploadResumeThreshold = 16 // force the resume-download path for this small test node

	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "big.bin", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path holds the server bytes, fetched via resume; the copy holds the old local bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "big.bin")); err != nil || !bytes.Equal(got, serverBytes) {
		t.Fatalf("canonical content mismatch (err %v)", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || !bytes.Equal(got, localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q set aside", got, err, localBytes)
	}
	// The chunked resume_download path carried the server node - not the single-frame Download.
	if c.resumeDLCalls == 0 {
		t.Fatalf("resume_download not used for large server node (resumeDLCalls=0)")
	}
	c.mu.Lock()
	offsets := append([]int64(nil), c.resumeDLOffsets...)
	c.mu.Unlock()
	wantChunks := (len(serverBytes) + 15) / 16
	if c.resumeDLCalls != wantChunks {
		t.Errorf("ResumeDownload called %d times, want %d", c.resumeDLCalls, wantChunks)
	}
	if len(offsets) == 0 || offsets[0] != 0 {
		t.Fatalf("offsets = %v, want first 0", offsets)
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Fatalf("offsets not strictly increasing: %v", offsets)
		}
	}

	// The canonical baseline records the server facts as synced.
	cb, err := st.GetBaseline(7, "big.bin")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" || cb.LocalHash != serverHash || cb.ServerHash != serverHash {
		t.Errorf("canonical baseline = %+v, want synced srv1/%s", cb, serverHash)
	}
	if cb.LocalSize != int64(len(serverBytes)) {
		t.Errorf("canonical baseline size = %d, want %d", cb.LocalSize, len(serverBytes))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

func TestResolveConflictSourceGone(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	// No local file on disk: the divergent source vanished in the scan→resolve window,
	// so the resolution degrades to "server wins" with no copy and no conflict record.
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: "stalehash", State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical path holds the server bytes; no copy or upload was made.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt", entries)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (source gone, nothing to preserve)", len(c.uploads))
	}
	// The canonical baseline adopted the server facts as synced.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "srv1" || b.ServerHash != serverHash || b.LocalHash != serverHash {
		t.Errorf("baseline = %+v, want synced srv1/%s", b, serverHash)
	}
	// No durable conflict record when there was no copy to preserve.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %d, want 0", len(recs))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

func TestResolveConflictServerIsDir(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	// The server replaced the path with a directory; keep-both is file-content only, so the
	// op must be dead-lettered (StateConflict) rather than downloading a dir node as a file.
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srvdir", IsDir: true, MTime: 555},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The op was dead-lettered: no copy, no upload, the local file is untouched.
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (dead-lettered)", len(c.uploads))
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt (no copy)", entries)
	}
	// The baseline is marked StateConflict and the queue is drained (not wedged).
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictServerEmptyFileID covers the guard for a QueryNode reply that carries no
// file id: a 24579 rejection means the server holds a diverged version, so an empty id is an
// unexpected reply. The resolver must leave the op queued to retry rather than record a baseline
// with no id or mutate the disk.
func TestResolveConflictServerEmptyFileID(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "", Hash: "deadbeef", MTime: 555, Size: 9},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The empty id is an unexpected reply: a retryable error, op left queued.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want an error for the empty file id, got nil")
	}

	// No disk mutation, no upload, no copy, and the op stays queued for the next drain.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (op deferred, not resolved)", len(c.uploads))
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt (no copy)", entries)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %v, want StatePending (unresolved, not dead-lettered)", b.State)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("queue len = %d, want 1 (op left queued to retry)", n)
	}
}

func TestResolveConflictParentConflicted(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	// The parent directory itself is stuck in StateConflict with no server id: the child can
	// never resolve its parent, so the op must be dead-lettered rather than retried forever.
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "dir", store.Baseline{
		FileType: "dir", State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if err := st.PutBaseline(7, "dir/a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "dir/a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// No server turn at all (QueryNode never called), no upload, queue drained.
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (dead-lettered before lease)", len(c.uploads))
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0 (parent-conflicted resolves before the lease)", len(c.queryNodeCalls))
	}
	b, err := st.GetBaseline(7, "dir/a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("child baseline state = %v, want StateConflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictParentUnknownLeavesQueued covers the transient-deferral branch: the
// child's parent directory has a baseline but no server id yet (StatePending, its own op has
// not run), so resolveParentID returns errParentUnknown. Unlike errParentConflicted this is
// NOT terminal - the op must stay queued (no server turn, no disk mutation) so the next drain
// retries once the parent's op records its id, mirroring TestOutboundParentUnknownLeavesQueued.
func TestResolveConflictParentUnknownLeavesQueued(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	// Parent baseline exists but has no server id and is StatePending (its mkdir has not run).
	if err := st.PutBaseline(7, "dir", store.Baseline{
		FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if err := st.PutBaseline(7, "dir/a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "dir/a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain surfaces the unknown-parent error; the op is retryable, not terminal.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want a retryable error for unknown parent, got nil")
	}

	// No server turn, no disk mutation, op stays queued, both baselines stay StatePending.
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (deferred before any work)", len(c.uploads))
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "dir", "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	b, err := st.GetBaseline(7, "dir/a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("child baseline state = %v, want StatePending (deferred, not dead-lettered)", b.State)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("queue len = %d, want 1 (op left queued to retry)", n)
	}
}

// TestResolveConflictRejectedPathDeadLettered confirms a resolve-conflict op whose path is
// not containable (a traversal escape) is rejected by the containment guard before any work
// and dead-lettered (acked, dropped) rather than wedging the queue - mirroring the outbound
// side's TestOutboundRejectedPathDeadLettered.
func TestResolveConflictRejectedPathDeadLettered(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "../escape", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The op is dead-lettered (errDeadLettered is swallowed by drainQueue), so no error and
	// the queue drains rather than retrying the bad path forever.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (rejected before any server turn)", len(c.uploads))
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0 (rejected before lease)", len(c.queryNodeCalls))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d (a rejected path must not wedge it)", n)
	}
	// The drop must leave a durable dead-letter trace so status reports it.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
}

func TestResolveConflictServerGone(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	// The diverged server node was deleted/moved after the 24579 rejection, so QueryNode
	// reports a path error. ErrPathError covers BOTH "node gone" and "operation timed out"
	// (errors.go), so the resolver confirms absence via an authoritative parent listing: a
	// clean listing that omits the node is proof it vanished. Only then does
	// it dead-letter (StateConflict) - rather than retry forever - freeing the shared path for
	// the authoritative inbound remove.
	c := &pushClient{
		queryNodeErr: actions.ErrPathError,
		listResult:   map[string][]actions.Node{"": {{Name: "other.txt", FileID: "f9"}}}, // omits a.txt → gone
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// No disk mutation: the local file is untouched and no copy was made or uploaded.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt (no copy)", entries)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (dead-lettered)", len(c.uploads))
	}
	// The parent listing was consulted to confirm absence before dead-lettering.
	if len(c.listCalls) == 0 {
		t.Errorf("parent listing not consulted; absence was not confirmed")
	}
	// The baseline is StateConflict and the queue is drained (not wedged).
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
	// The terminal unresolvable drop must leave a durable dead-letter trace.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped unresolvable resolve-conflict op", n, err)
	}
}

// TestResolveConflictServerParentGone covers a resolver wedge: the diverged
// node's whole parent directory was deleted server-side after the 24579 rejection, so
// QueryNode(op.Path) AND List(parent) both report ErrPathError. Before the ancestor walk the
// absence stayed inconclusive forever while the authoritative inbound remove of the parent
// serialised BEHIND this resolve on the shared path - a mutual, permanent wedge. The walk turns
// the deleted parent into positive proof of absence (the view root lists cleanly and omits it),
// so the op dead-letters through the errConflictServerGone path (baseline StateConflict, durable
// trace, queue drained) and the inbound remove is unblocked.
func TestResolveConflictServerParentGone(t *testing.T) {
	ts := time.Date(2026, 7, 15, 10, 0, 0, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{
		// Both deleted paths answer the sentinel, so each rung of the walk corroborates its
		// listing-derived verdict with a probe of its own path.
		queryNodeErrByPath: map[string]error{"d/a.txt": actions.ErrPathError, "d": actions.ErrPathError},
		listErrByPath:      map[string]error{"d": actions.ErrPathError},
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "f9"}}}, // omits d → parent gone
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.Mkdir(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "d", "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "d", store.Baseline{FileID: "fd", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if err := st.PutBaseline(7, "d/a.txt", store.Baseline{
		FileID: "f2", FileType: "file", LocalHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7,
		Path: "d/a.txt", FileID: "f2", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want the resolve to dead-letter, not retry forever)", err)
	}

	// No disk mutation: the local file is untouched and no copy was made or uploaded.
	if got, err := os.ReadFile(filepath.Join(viewDir, "d", "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (dead-lettered)", len(c.uploads))
	}
	b, err := st.GetBaseline(7, "d/a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d (the wedge this task closes)", n)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
	// The ancestor walk consulted the deleted parent's listing, then the view root's - once per
	// drain pass, since the absence corroboration now needs agreeing probes across passes
	// and each pass re-runs the same walk.
	// The walk terminates at the view root, addressed as "/" (viewRootPath) - the
	// spelling a live DSM accepts; the empty path is rejected with 28672.
	wantLists := []string{"d", viewRootPath}
	if len(c.listCalls) == 0 || len(c.listCalls)%len(wantLists) != 0 {
		t.Fatalf("list calls = %q, want whole repetitions of %q", c.listCalls, wantLists)
	}
	for i, got := range c.listCalls {
		if got != wantLists[i%len(wantLists)] {
			t.Fatalf("list calls = %q, want repetitions of %q", c.listCalls, wantLists)
		}
	}
}

// TestResolveConflictServerQueryTimeout covers the OTHER meaning of ErrPathError: a
// server-side operation timeout, not a vanished node. The parent listing is inconclusive
// (it times out too), so the resolver must leave the op QUEUED to retry rather than
// dead-letter a still-unresolved conflict on a transient blip.
func TestResolveConflictServerQueryTimeout(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{
		queryNodeErr: actions.ErrPathError, // ambiguous: absent or timeout
		listErr:      actions.ErrPathError, // the parent listing is inconclusive (a timeout too)
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain surfaces the path error (the op is retryable, not terminal).
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want a retryable error, got nil")
	}

	// No disk mutation, no upload, and the op stays queued for the next drain.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (op deferred, not resolved)", len(c.uploads))
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt (no copy)", entries)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %v, want StatePending (unresolved, not dead-lettered)", b.State)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("queue len = %d, want 1 (op left queued to retry)", n)
	}
}

// TestResolveConflictServerRenamedAwayNotGone covers the id-based gone-detection the resolve
// op's FileID enables: QueryNode of the canonical path returns ErrPathError because the leaf
// NAME disappeared, but the diverged server node lives on under a new name - the parent
// listing still contains a child bearing the op's FileID. A name-only check (the behaviour
// when the resolve op carries no FileID) would read the missing leaf as proof of absence and
// wrongly dead-letter a real, still-resolvable divergence. With the FileID carried through the
// funnel, confirmAbsentByParentList matches the node by id, reports it still present, and the
// op stays queued to retry once the authoritative inbound rename relocates the canonical path.
func TestResolveConflictServerRenamedAwayNotGone(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{
		queryNodeErr: actions.ErrPathError, // the leaf name is gone from the canonical path
		// ...but the node lives on under a new name: a child still bears the op's FileID.
		listResult: map[string][]actions.Node{"": {{Name: "renamed.txt", FileID: "srvf"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srvf", FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	// The resolve op carries the server FileID, exactly as funnelUploadConflict now enqueues it.
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", FileID: "srvf", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The drain surfaces the path error: the node is still present (renamed), so the op is
	// retryable, not terminal.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want a retryable error (node renamed, not gone), got nil")
	}

	// No disk mutation, no upload, op stays queued, baseline stays StatePending (not buried).
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (op deferred, not resolved)", len(c.uploads))
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StatePending {
		t.Errorf("baseline state = %v, want StatePending (still resolvable, not dead-lettered)", b.State)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("queue len = %d, want 1 (op left queued to retry)", n)
	}
}

func TestResolveConflictReadOnlyView(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	// The view was downgraded to read-only after the resolve op was enqueued: keep-both
	// cannot push the copy, so the resolver must dead-letter BEFORE any disk mutation rather
	// than rename the local file aside, overwrite the canonical path, then fail uploading the
	// copy.
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1", Privilege: "ro"}); err != nil {
		t.Fatalf("downgrade view: %v", err)
	}

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// No server turn (QueryNode never reached), no upload, the local file is untouched.
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (dead-lettered before any mutation)", len(c.uploads))
	}
	if len(c.queryNodeCalls) != 0 {
		t.Errorf("QueryNode calls = %d, want 0 (read-only guard runs before the lease)", len(c.queryNodeCalls))
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt (no copy)", entries)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
	// The read-only drop must leave a durable dead-letter trace so status reports it.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped read-only resolve-conflict op", n, err)
	}
}

func TestResolveConflictCopyNameCollision(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	deterministicRel := conflictCopyName("a.txt", host, ts)
	deterministicName := filepath.Base(deterministicRel)
	// The no-clobber search appends "_1" before the extension when the deterministic name
	// is taken.
	uniqueRel := conflictCopyNameN("a.txt", host, ts, 1)
	uniqueName := filepath.Base(uniqueRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		uploadNode:      map[string]actions.Node{uniqueName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	// A pre-existing file already occupies the deterministic conflict-copy name (a real user
	// file, or a same-second prior conflict copy). It must NOT be overwritten.
	preexisting := []byte("do-not-clobber")
	if err := os.WriteFile(filepath.Join(viewDir, deterministicName), preexisting, 0o644); err != nil {
		t.Fatalf("seed pre-existing copy name: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The pre-existing file at the deterministic name is intact.
	if got, err := os.ReadFile(filepath.Join(viewDir, deterministicName)); err != nil || string(got) != string(preexisting) {
		t.Fatalf("pre-existing copy = %q (err %v), want untouched %q", got, err, preexisting)
	}
	// The divergent local content was preserved under the uniquified name.
	if got, err := os.ReadFile(filepath.Join(viewDir, uniqueName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("uniquified copy = %q (err %v), want %q", got, err, localBytes)
	}
	// The canonical path holds the server bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, serverBytes)
	}
	// The copy was uploaded under the uniquified name.
	if len(c.uploads) != 1 || c.uploads[0].name != uniqueName {
		t.Fatalf("uploads = %+v, want one upload of %q", c.uploads, uniqueName)
	}
	// The copy baseline and conflict record use the uniquified path.
	pb, err := st.GetBaseline(7, uniqueRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if pb.State != store.StateSynced || pb.FileID != "copyf" {
		t.Errorf("copy baseline = %+v, want synced copyf", pb)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CopyPath != uniqueRel {
		t.Fatalf("conflict records = %+v, want one with copy %q", recs, uniqueRel)
	}
}

func TestResolveConflictCanonicalDownloadNoFileID(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	// The download reply omits the node id; the canonical baseline must fall back to the
	// queried server.FileID rather than recording an empty (unindexed) id.
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}}, // no FileID
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	cb, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" {
		t.Errorf("canonical baseline = %+v, want synced with fallback FileID srv1", cb)
	}
	// The id must be byid-indexed so a later server move/delete can find it.
	if p, err := st.PathByID(7, "srv1"); err != nil || p != "a.txt" {
		t.Errorf("PathByID(srv1) = %q (err %v), want a.txt", p, err)
	}
}

func TestResolveConflictCopyNoFileID(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)

	// The copy upload succeeds but its reply omits a file id, and the recovery QueryNode for
	// the copy path also returns nothing (queryNode has no entry for copyRel). The resolver
	// must NOT record a synced-but-unindexed copy baseline; it leaves the copy for the next
	// local scan to re-push with a readable id, while still recording provenance.
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		// uploadNode unset for the copy name → Upload returns a zero Node (no file id).
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The copy was uploaded and preserved on disk.
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want 1", len(c.uploads))
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.Base(copyRel))); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// No copy baseline was recorded (an empty FileID would be unindexed and never re-pushed).
	if _, err := st.GetBaseline(7, copyRel); !isNotFound(err) {
		t.Errorf("GetBaseline copy err = %v, want not-found (baseline deferred to next scan)", err)
	}
	// Provenance is still recorded (CopyFileID empty is informational).
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CopyPath != copyRel || recs[0].CopyFileID != "" {
		t.Fatalf("conflict records = %+v, want one with copy %q and empty CopyFileID", recs, copyRel)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictCopyNoFileIDRecovered covers the opposite branch of
// TestResolveConflictCopyNoFileID: the copy upload reply omits a file id, but the recovery
// QueryNode for the copy path SUCCEEDS, so the resolver adopts the recovered id/hash/mtime into
// a synced, indexed copy baseline rather than deferring it to the next local scan.
func TestResolveConflictCopyNoFileIDRecovered(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)

	// The copy upload returns a zero Node (no file id), but the recovery QueryNode(copyRel)
	// returns a real node, so its id/hash/mtime are adopted into the copy baseline.
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
			copyRel: {FileID: "copyrecovered", Hash: localHash, MTime: 888, Size: int64(len(localBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		// uploadNode unset for the copy name → Upload returns a zero Node (no file id).
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The copy baseline is recorded as synced with the RECOVERED facts (id/hash/mtime).
	b, err := st.GetBaseline(7, copyRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("copy baseline state = %v, want StateSynced", b.State)
	}
	if b.FileID != "copyrecovered" {
		t.Errorf("copy baseline FileID = %q, want copyrecovered", b.FileID)
	}
	if b.ServerHash != localHash || b.ServerMTime != 888 {
		t.Errorf("copy baseline server facts = (%q, %d), want (%q, 888)", b.ServerHash, b.ServerMTime, localHash)
	}
	// The byid index records the recovered id so a later server move/delete can find it.
	if p, err := st.PathByID(7, "copyrecovered"); err != nil || p != copyRel {
		t.Errorf("PathByID(copyrecovered) = %q (err %v), want %q", p, err, copyRel)
	}
	// Provenance carries the recovered CopyFileID.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CopyFileID != "copyrecovered" {
		t.Fatalf("conflict records = %+v, want one with CopyFileID copyrecovered", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestResolveConflictCopyUploadFailsRecordsProvenance verifies the durable Conflict record is
// written as soon as the divergent local file is set aside - before the copy upload - so a
// transient copy-upload failure cannot permanently drop provenance. On a retry the canonical
// path already holds the server version, so the resolver would take the false-conflict fast path
// and ack; a record written only at the end of resolveConflict would be lost forever.
func TestResolveConflictCopyUploadFailsRecordsProvenance(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
		// The copy upload fails transiently, after the local file has been set aside and the
		// canonical path overwritten with the server version.
		uploadErr: errors.New("transient network blip"),
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The transient copy-upload failure surfaces from the drain and leaves the op queued.
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want error from transient copy-upload failure, got nil")
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("queue len = %d, want 1 (op left queued for retry)", n)
	}

	// The local file was set aside under its copy name...
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.Base(copyRel))); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// ...and provenance is ALREADY durable even though the op never completed.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Fatalf("conflict records = %+v, want one for a.txt copy %q", recs, copyRel)
	}
}

// TestResolveConflictSparseReplies covers sparse replies: both the canonical download
// reply and the copy upload reply omit hash/size/mtime (decodeNode is tolerant). The
// resolver must record COMPLETE synced baselines anyway - the canonical one falling back
// to the full QueryNode server facts, the copy one falling back to the locally-computed
// content hash - so a later local scan does not see a phantom change on a hash/size/mtime
// of zero.
func TestResolveConflictSparseReplies(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// QueryNode carries the complete server facts (id/hash/size/mtime).
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		// The download reply is sparse: no hash/size/mtime (and no id).
		downloadNode: map[string]actions.Node{"srv1": {}},
		// The copy upload reply carries only a file id - no hash/mtime.
		uploadNode: map[string]actions.Node{copyName: {FileID: "copyf"}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The canonical baseline filled hash/size/mtime from the QueryNode server facts despite
	// the sparse download reply.
	cb, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" ||
		cb.ServerHash != serverHash || cb.LocalHash != serverHash ||
		cb.ServerMTime != 555 || cb.LocalMTime != 555 || cb.LocalSize != int64(len(serverBytes)) {
		t.Errorf("canonical baseline = %+v, want synced srv1 with full server facts (hash %s, mtime 555, size %d)",
			cb, serverHash, len(serverBytes))
	}

	// The copy baseline filled its server hash from the local content hash despite the sparse
	// upload reply, so it is a complete synced record (not a phantom-change trap).
	pb, err := st.GetBaseline(7, copyRel)
	if err != nil {
		t.Fatalf("GetBaseline copy: %v", err)
	}
	if pb.State != store.StateSynced || pb.FileID != "copyf" ||
		pb.ServerHash != localHash || pb.LocalHash != localHash ||
		pb.LocalSize != int64(len(localBytes)) || pb.ServerMTime == 0 {
		t.Errorf("copy baseline = %+v, want synced copyf with server hash %s and non-zero mtime", pb, localHash)
	}
}

// TestResolveConflictSparseDownloadContentChanged guards a sparse reply: when the
// server file changes content between QueryNode and Download (a file id is stable across
// modifications) and the sparse download reply omits the hash/size, the canonical baseline
// must describe the bytes ACTUALLY downloaded onto disk - not the now-stale QueryNode
// metadata. Otherwise the baseline would disagree with disk and the next scan would
// re-detect a phantom change (or, worse, the local hash would never match the disk bytes).
func TestResolveConflictSparseDownloadContentChanged(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	// QueryNode advertises an old version; Download returns a newer one under the same id.
	staleBytes := []byte("server-version-as-queried")
	freshBytes := []byte("server-version-after-a-concurrent-edit")
	localBytes := []byte("local-version-divergent")
	staleHash := md4.ContentHash(staleBytes)
	freshHash := md4.ContentHash(freshBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// QueryNode carries the STALE facts (hash/size/mtime) for the file id.
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: staleHash, MTime: 555, Size: int64(len(staleBytes))},
		},
		// Download returns the FRESH content but a sparse reply (no hash/size/mtime/id).
		downloadContent: map[string][]byte{"srv1": freshBytes},
		downloadNode:    map[string]actions.Node{"srv1": {}},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// Disk holds the freshly downloaded bytes.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(freshBytes) {
		t.Fatalf("canonical content = %q (err %v), want %q", got, err, freshBytes)
	}

	// The baseline must record the DISK (fresh) hash/size, not the stale QueryNode facts.
	cb, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" {
		t.Fatalf("canonical baseline = %+v, want synced srv1", cb)
	}
	if cb.ServerHash != freshHash || cb.LocalHash != freshHash {
		t.Errorf("canonical hash = server %q / local %q, want %q (downloaded bytes), not stale %q",
			cb.ServerHash, cb.LocalHash, freshHash, staleHash)
	}
	if cb.LocalSize != int64(len(freshBytes)) {
		t.Errorf("canonical size = %d, want %d (downloaded bytes)", cb.LocalSize, len(freshBytes))
	}
	// mtime fell back to the QueryNode value and was written to disk too.
	if cb.LocalMTime != 555 || cb.ServerMTime != 555 {
		t.Errorf("canonical mtime = local %d / server %d, want 555", cb.LocalMTime, cb.ServerMTime)
	}
	if fi, err := os.Stat(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Fatalf("stat canonical: %v", err)
	} else if fi.ModTime().Unix() != 555 {
		t.Errorf("on-disk mtime = %d, want 555 (resolved fallback, not epoch)", fi.ModTime().Unix())
	}
}

func TestResolveConflictFalsePositive(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	body := []byte("identical")
	hash := md4.ContentHash(body)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: hash, MTime: 555, Size: int64(len(body))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: hash, LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// No copy made, no upload issued - the local file already matched the server.
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %d, want 0 (false conflict)", len(c.uploads))
	}
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "a.txt" {
		t.Errorf("view dir entries = %v, want just a.txt", entries)
	}
	// The baseline adopted the server facts as synced.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "srv1" || b.ServerHash != hash {
		t.Errorf("baseline = %+v, want synced srv1/%s", b, hash)
	}
	// No durable conflict record for a false conflict.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %d, want 0", len(recs))
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d", n)
	}
}

// TestAdoptFalseConflictPhase exercises the extracted adoptFalseConflict phase directly:
// it must adopt the server facts onto the baseline as StateSynced (carrying the op's
// ServerSyncID) without writing any conflict record or touching disk - recording the ACTUAL
// on-disk (size, mtime) it is passed rather than the server's mtime, so the Phase-9d cheap
// scan hint matches the untouched file.
func TestAdoptFalseConflictPhase(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")

	server := actions.Node{FileID: "srv9", Hash: "abc", MTime: 123, Size: 42}
	op := store.Op{ViewID: 7, Path: "a.txt", UnixPerm: 644, ServerSyncID: 88}
	const localSize, localMTime int64 = 42, 999
	if err := e.adoptFalseConflict(op, server, server.Hash, localSize, localMTime); err != nil {
		t.Fatalf("adoptFalseConflict: %v", err)
	}

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.FileID != "srv9" || b.ServerHash != "abc" ||
		b.LocalHash != "abc" || b.LocalSize != localSize || b.ServerMTime != 123 ||
		b.LocalMTime != localMTime || b.UnixPerm != 644 || b.ServerSyncID != 88 {
		t.Errorf("baseline = %+v, want synced srv9 adopting server facts with on-disk mtime %d", b, localMTime)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %d, want 0 for a false conflict", len(recs))
	}
}

// TestAdoptFalseConflictMonotonicServerSyncID pins the monotonic guard on the
// false-conflict stamp: a resolve op carrying an OLDER sync id than the baseline records (a
// funneled resolve carries the triggering download's - older - event id) must NOT rewind the
// recorded value, since a self-echo move may have overlaid a newer id whose "strictly newer"
// proof the crash-replay redirect gate (downloadRelocatedBySelfEcho) depends on; a newer id
// still advances it, and a zero id (outbound/cold-start funnels) still never zeroes it.
func TestAdoptFalseConflictMonotonicServerSyncID(t *testing.T) {
	e, st, _ := outboundEngine(t, &pushClient{}, "Photos", "root1")

	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv9", FileType: "file", State: store.StateConflict, ServerSyncID: 100,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	server := actions.Node{FileID: "srv9", Hash: "abc", MTime: 123, Size: 42}

	// Older op id (88 < 100): the recorded position must not rewind.
	op := store.Op{ViewID: 7, Path: "a.txt", UnixPerm: 644, ServerSyncID: 88}
	if err := e.adoptFalseConflict(op, server, server.Hash, 42, 999); err != nil {
		t.Fatalf("adoptFalseConflict(older): %v", err)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.ServerSyncID != 100 {
		t.Errorf("baseline after older-id adopt = %+v, want synced with ServerSyncID 100 (not rewound to 88)", b)
	}

	// Zero op id (outbound/cold-start funnel): still never zeroes the recorded value.
	op.ServerSyncID = 0
	if err := e.adoptFalseConflict(op, server, server.Hash, 42, 999); err != nil {
		t.Fatalf("adoptFalseConflict(zero): %v", err)
	}
	if b, err = st.GetBaseline(7, "a.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.ServerSyncID != 100 {
		t.Errorf("ServerSyncID after zero-id adopt = %d, want 100 (never zeroed)", b.ServerSyncID)
	}

	// Newer op id (150 > 100): advances.
	op.ServerSyncID = 150
	if err := e.adoptFalseConflict(op, server, server.Hash, 42, 999); err != nil {
		t.Fatalf("adoptFalseConflict(newer): %v", err)
	}
	if b, err = st.GetBaseline(7, "a.txt"); err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.ServerSyncID != 150 {
		t.Errorf("ServerSyncID after newer-id adopt = %d, want 150", b.ServerSyncID)
	}
}

// TestDownloadCanonicalMonotonicServerSyncID pins the same monotonic guard on the
// real-conflict canonical stamp (downloadCanonical's markSynced): an older funneled sync id
// must not rewind the baseline's recorded server position, a newer one advances it.
func TestDownloadCanonicalMonotonicServerSyncID(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	body := []byte("server wins")
	hash := md4.ContentHash(body)

	for _, tc := range []struct {
		name     string
		path     string
		opSyncID int64
		want     int64
	}{
		{name: "older id does not rewind", path: "low.txt", opSyncID: 88, want: 100},
		{name: "newer id advances", path: "high.txt", opSyncID: 150, want: 150},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{
				downloadContent: map[string][]byte{"srv9": body},
				downloadNode: map[string]actions.Node{
					"srv9": {FileID: "srv9", Hash: hash, Size: int64(len(body)), MTime: 123},
				},
			}
			e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
			if err := st.PutBaseline(7, tc.path, store.Baseline{
				FileID: "srv9", FileType: "file", State: store.StateConflict, ServerSyncID: 100,
			}); err != nil {
				t.Fatalf("seed baseline: %v", err)
			}

			// The canonical path is free on disk (preserveAndRecord set the divergent local
			// copy aside before downloadCanonical runs in the real resolver flow).
			op := store.Op{ViewID: 7, Path: tc.path, UnixPerm: 644, ServerSyncID: tc.opSyncID}
			server := actions.Node{FileID: "srv9", Hash: hash, Size: int64(len(body)), MTime: 123}
			abs := filepath.Join(viewDir, tc.path)
			if err := e.downloadCanonical(context.Background(), c, op, server, abs, ts); err != nil {
				t.Fatalf("downloadCanonical: %v", err)
			}

			b, err := st.GetBaseline(7, tc.path)
			if err != nil {
				t.Fatalf("GetBaseline: %v", err)
			}
			if b.State != store.StateSynced || b.ServerSyncID != tc.want {
				t.Errorf("baseline = %+v, want synced with ServerSyncID %d", b, tc.want)
			}
			got, rerr := os.ReadFile(abs)
			if rerr != nil || !bytes.Equal(got, body) {
				t.Errorf("canonical on disk = %q/%v, want server bytes", got, rerr)
			}
		})
	}
}

// TestResolveFalseConflictCheapScanShortCircuits proves that after a
// false-conflict adoption (which never touches the file) the baseline records the file's ACTUAL
// on-disk (size, mtime), so the Phase-9d cheap scan short-circuits on the hint instead of
// re-opening and re-hashing the file on every pass. The server reports a DIFFERENT mtime than the
// file carries on disk; before the fix the baseline stored the server mtime and the hint never
// matched.
func TestResolveFalseConflictCheapScanShortCircuits(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	body := []byte("identical")
	hash := md4.ContentHash(body)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			// Server mtime 555 diverges from the on-disk mtime pinned below.
			"a.txt": {FileID: "srv1", Hash: hash, MTime: 555, Size: int64(len(body))},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	abs := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(abs, body, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	onDisk := time.Unix(1_000_000, 0)
	if err := os.Chtimes(abs, onDisk, onDisk); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", LocalHash: hash, LocalSize: int64(len(body)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The adopted baseline must carry the file's on-disk mtime, not the server's 555.
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Fatalf("baseline state = %v, want synced", b.State)
	}
	if b.LocalMTime != onDisk.Unix() || b.LocalSize != int64(len(body)) {
		t.Fatalf("baseline local facts = (size %d, mtime %d), want on-disk (%d, %d)",
			b.LocalSize, b.LocalMTime, len(body), onDisk.Unix())
	}

	// Rewrite the content with different bytes of the SAME size and restore the mtime: a cheap
	// scan that short-circuits on the hint reuses the recorded hash, while a full re-hash would
	// surface the new content's hash. Build the hints exactly as localScan does.
	changed := []byte("different") // len 9 == len("identical")
	if len(changed) != len(body) {
		t.Fatalf("test bug: content sizes differ")
	}
	if err := os.WriteFile(abs, changed, 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := os.Chtimes(abs, onDisk, onDisk); err != nil {
		t.Fatalf("Chtimes restore: %v", err)
	}

	hints := map[string]scanHint{}
	if _, _, _, _, err := e.collectBaselines(7, hints); err != nil {
		t.Fatalf("collectBaselines: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), 7, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	var found *scanEntry
	for i := range entries {
		if entries[i].relPath == "a.txt" {
			found = &entries[i]
		}
	}
	if found == nil {
		t.Fatalf("a.txt not in scan entries %+v", entries)
	}
	if found.md4 != hash {
		t.Errorf("scan md4 = %q, want cheap-scan hint hash %q (a re-hash would give %q)",
			found.md4, hash, md4.ContentHash(changed))
	}
}

// TestPreserveAndRecordPhase exercises the extracted preserveAndRecord phase directly: it
// moves the divergent local file aside to a free _Conflict copy, frees the canonical path,
// and writes a durable conflict provenance record naming the chosen copy.
func TestPreserveAndRecordPhase(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir := outboundEngine(t, &pushClient{}, "Photos", "root1")
	pinClock(e, host, ts)

	localBytes := []byte("divergent-local")
	abs := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(abs, localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}

	copyRel, copyAbs, err := e.preserveAndRecord(7, "a.txt", abs, ts)
	if err != nil {
		t.Fatalf("preserveAndRecord: %v", err)
	}
	wantCopy := conflictCopyName("a.txt", host, ts)
	if copyRel != wantCopy {
		t.Errorf("copyRel = %q, want %q", copyRel, wantCopy)
	}
	// The canonical path is now free and the copy holds the divergent bytes.
	if _, err := os.Stat(abs); !os.IsNotExist(err) {
		t.Errorf("canonical path still present (err %v), want freed", err)
	}
	if got, err := os.ReadFile(copyAbs); err != nil || string(got) != string(localBytes) {
		t.Errorf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	// A durable conflict record names the copy.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != wantCopy {
		t.Errorf("conflict records = %+v, want one naming %q", recs, wantCopy)
	}
}
