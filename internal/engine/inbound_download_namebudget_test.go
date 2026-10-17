package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// These tests pin the download staging-name budget. The download promote staged into
// os.CreateTemp(dir, "."+base+".dl-*"), which inflates the target's base name by ~14
// bytes: a name at the filesystem's per-component limit produced a staging name PAST it.
// The resulting ENAMETOOLONG was classified as transient, so the op retried every drain
// forever, escalated to Stuck and - through blockedKeys - wedged every op whose path
// overlapped it. The failure was self-inflicted: conflictCopyName deliberately emits
// names of exactly maxCopyNameComponentBytes, so this client's own keep-both copy could
// never be downloaded back (by a second client, or by this one after a re-pair). The fix
// budgets the staging name (stagingTempPattern) and, as a backstop for names the SERVER
// supplies that the local filesystem cannot hold at all, dead-letters a name-too-long
// failure instead of retrying it forever (nameTooLong + applyOp's terminal classifier).

// requireLongComponentNames skips unless the host can hold a path component of the full
// maxCopyNameComponentBytes budget - the premise of every fixture below. A filesystem
// with a shorter NAME_MAX (or an eCryptfs-style name-expanding layer) would otherwise
// fail these tests for a reason that says nothing about the code under test.
func requireLongComponentNames(t *testing.T, dir string) {
	t.Helper()
	probe := filepath.Join(dir, strings.Repeat("p", maxCopyNameComponentBytes))
	f, err := os.Create(probe)
	if err != nil {
		t.Skipf("host cannot hold a %d-byte path component: %v", maxCopyNameComponentBytes, err)
	}
	_ = f.Close()
	_ = os.Remove(probe)
}

// requireNameTooLongRejection skips unless the host rejects an absurdly long component
// with an error nameTooLong classifies - the premise of the dead-letter fixture. It
// returns the rejected base name.
func requireNameTooLongRejection(t *testing.T) string {
	t.Helper()
	name := strings.Repeat("n", 4*maxCopyNameComponentBytes)
	probe := filepath.Join(t.TempDir(), name)
	f, err := os.Create(probe)
	if err == nil {
		_ = f.Close()
		_ = os.Remove(probe)
		t.Skipf("host accepts a %d-byte path component; nothing to classify", len(name))
	}
	if !nameTooLong(err) {
		t.Skipf("host rejects an over-long component with %v, which nameTooLong does not classify", err)
	}
	return name
}

// TestStagingTempPatternBudget pins the arithmetic: whatever CreateTemp appends to the
// pattern (a decimal uint32, so at most stagingRandomMaxBytes digits), the longest name
// it can generate stays within maxCopyNameComponentBytes, the marker and the leading dot
// survive truncation (so watcher/filter.go still recognises the temp as self-induced),
// and a multibyte rune is never cut in half.
func TestStagingTempPatternBudget(t *testing.T) {
	cases := []struct {
		name string
		base string
	}{
		{"short", "a.txt"},
		{"empty", ""},
		{"at budget", strings.Repeat("b", maxStagingBaseBytes)},
		{"one over budget", strings.Repeat("b", maxStagingBaseBytes+1)},
		{"at component limit", strings.Repeat("b", maxCopyNameComponentBytes)},
		{"multibyte over budget", strings.Repeat("é", maxCopyNameComponentBytes)},
		{"four-byte runes over budget", strings.Repeat("🙂", maxCopyNameComponentBytes)},
		{"conflict copy at limit", filepath.Base(conflictCopyName(
			strings.Repeat("c", maxCopyNameComponentBytes)+".txt", "workstation-01", time.Unix(1_700_000_000, 0)))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pat := stagingTempPattern(tc.base)
			prefix, ok := strings.CutSuffix(pat, "*")
			if !ok {
				t.Fatalf("pattern %q does not end in the CreateTemp wildcard", pat)
			}
			// The worst case CreateTemp can generate: the pattern's prefix plus the widest
			// decimal a uint32 reaches.
			if got := len(prefix) + stagingRandomMaxBytes; got > maxCopyNameComponentBytes {
				t.Errorf("worst-case staging name = %d bytes, want <= %d", got, maxCopyNameComponentBytes)
			}
			if !strings.HasPrefix(pat, stagingDotPrefix) {
				t.Errorf("pattern %q lost its hidden-file dot", pat)
			}
			if !strings.HasSuffix(prefix, stagingMarker) {
				t.Errorf("pattern %q lost the %q marker the watcher filter matches on", pat, stagingMarker)
			}
			if !utf8.ValidString(pat) {
				t.Errorf("pattern %q truncated a multibyte rune in half", pat)
			}
			if len(tc.base) <= maxStagingBaseBytes && !strings.Contains(pat, tc.base) {
				t.Errorf("pattern %q dropped part of the within-budget base %q", pat, tc.base)
			}
		})
	}
}

// seedLongNameDownload seeds a pending baseline plus a queued inbound download of rel and
// returns the content the fake server serves for it.
func seedLongNameDownload(t *testing.T, st *store.Store, c *pushClient, rel, fileID string) []byte {
	t.Helper()
	content := []byte("server content for " + fileID)
	hash := md4.ContentHash(content)
	c.downloadContent[fileID] = content
	c.downloadNode[fileID] = actions.Node{
		FileID: fileID, Hash: hash, Size: int64(len(content)), MTime: 1_700_000_000,
	}
	if err := st.PutBaseline(7, rel, store.Baseline{
		FileID: fileID, FileType: "file", ServerHash: hash, State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7,
		Path: rel, FileID: fileID, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return content
}

// assertDownloadLanded checks the download of rel promoted: the content is on disk, the
// baseline is synced, nothing is left queued and nothing was dead-lettered. Before the fix
// the staging CreateTemp failed with ENAMETOOLONG, leaving the op queued forever.
func assertDownloadLanded(t *testing.T, e *Engine, st *store.Store, viewDir, rel string, want []byte) {
	t.Helper()
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read promoted file: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("promoted content = %q, want %q", got, want)
	}
	b, err := st.GetBaseline(7, rel)
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("baseline state = %v, want StateSynced", b.State)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0: the op is retrying forever", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0", n, err)
	}
	// The staging temp is always removed, whatever name it was budgeted down to.
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(viewDir, filepath.FromSlash(rel))))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, en := range entries {
		if strings.Contains(en.Name(), stagingMarker) {
			t.Errorf("staging temp %q left behind", en.Name())
		}
	}
}

// TestInboundDownloadMaxLengthBasenamePromotes: a server file whose base name is exactly
// at the per-component limit downloads and promotes. Before the budget the staging temp
// name overflowed the limit and the op never applied.
func TestInboundDownloadMaxLengthBasenamePromotes(t *testing.T) {
	c := &pushClient{downloadContent: map[string][]byte{}, downloadNode: map[string]actions.Node{}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	requireLongComponentNames(t, viewDir)

	rel := strings.Repeat("l", maxCopyNameComponentBytes-len(".txt")) + ".txt"
	if len(rel) != maxCopyNameComponentBytes {
		t.Fatalf("fixture base name = %d bytes, want %d", len(rel), maxCopyNameComponentBytes)
	}
	want := seedLongNameDownload(t, st, c, rel, "f-long")
	assertDownloadLanded(t, e, st, viewDir, rel, want)
}

// TestInboundDownloadConflictCopyNameDownloadsBack is the self-inflicted case: the name
// this client's own keep-both resolver produces (exactly maxCopyNameComponentBytes, by
// construction) is what a second client - or this one after a re-pair and cold start -
// must be able to download. Before the budget that download wedged its whole path chain.
func TestInboundDownloadConflictCopyNameDownloadsBack(t *testing.T) {
	c := &pushClient{downloadContent: map[string][]byte{}, downloadNode: map[string]actions.Node{}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	requireLongComponentNames(t, viewDir)

	rel := conflictCopyName(strings.Repeat("r", maxCopyNameComponentBytes)+".txt",
		"workstation-01", time.Unix(1_700_000_000, 0))
	if len(rel) != maxCopyNameComponentBytes {
		t.Fatalf("conflict copy name = %d bytes, want the full budget %d", len(rel), maxCopyNameComponentBytes)
	}
	want := seedLongNameDownload(t, st, c, rel, "f-conflict")
	assertDownloadLanded(t, e, st, viewDir, rel, want)
}

// TestNameTooLongClassification pins the classifier itself: the POSIX errno and every
// per-OS equivalent are recognised through wrapping, and an unrelated error is not.
func TestNameTooLongClassification(t *testing.T) {
	for i, target := range append([]error{syscall.ENAMETOOLONG}, extraNameTooLongErrors...) {
		if !nameTooLong(target) {
			t.Errorf("nameTooLong(%v) = false, want true (target %d)", target, i)
		}
		if !nameTooLong(fmt.Errorf("engine: stat %q: %w", "x", target)) {
			t.Errorf("nameTooLong lost target %d (%v) through wrapping", i, target)
		}
	}
	if nameTooLong(os.ErrPermission) {
		t.Error("nameTooLong(ErrPermission) = true, want false: a permission failure stays transient")
	}
	if nameTooLong(nil) {
		t.Error("nameTooLong(nil) = true, want false")
	}
}

// TestDrainQueueDeadLettersUnappliableLongName: a name the local filesystem rejects
// outright (a server-supplied component past the local limit - DSM's limits are not the
// host's) is dead-lettered with a durable trace instead of retrying every drain. The
// witness that the chain is not wedged is the op queued BEHIND it on an overlapping path:
// it applies in the same drain, which a transient classification would have blocked.
func TestDrainQueueDeadLettersUnappliableLongName(t *testing.T) {
	longBase := requireNameTooLongRejection(t)
	c := &pushClient{downloadContent: map[string][]byte{}, downloadNode: map[string]actions.Node{}}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	// The parent must exist so the path walk reaches (and rejects) the long component
	// rather than stopping at an absent parent.
	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed parent dir: %v", err)
	}

	rel := "d/" + longBase
	seedLongNameDownload(t, st, c, rel, "f-toolong")
	// A directory op on the download's ANCESTOR path: it overlaps (serialises behind the
	// download) and must still apply once the poison op is dropped.
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fd", FileType: "dir", State: store.StatePending,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirInbound, ViewID: 7, Path: "d", FileID: "fd", UnixPerm: 755,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should drop an unappliable name, not wedge: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the unappliable op still wedges its chain", n, err)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want 1 durable trace for the unappliable name", len(dls))
	}
	if dls[0].Path != rel {
		t.Errorf("dead-lettered op path = %q, want %q", dls[0].Path, rel)
	}
	if dls[0].Reason == "" {
		t.Error("dead-letter trace carries no reason")
	}
	// The never-materialised file's baseline is resolved (no stranded pending record).
	if _, err := st.GetBaseline(7, rel); !isNotFound(err) {
		t.Errorf("pending baseline for the dropped download survived, got err %v", err)
	}
	// The chain behind it flowed in the SAME drain: the ancestor mkdir applied.
	b, err := st.GetBaseline(7, "d")
	if err != nil {
		t.Fatalf("GetBaseline(d): %v", err)
	}
	if b.State != store.StateSynced {
		t.Errorf("ancestor mkdir baseline state = %v, want StateSynced (its chain was blocked)", b.State)
	}
}
