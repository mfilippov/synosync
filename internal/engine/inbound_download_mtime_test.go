package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// These tests close an mtime-hint gap: markSynced recorded the raw wire mtime as the baseline's
// LocalMTime even when SetMeta (fsutil) warned and SKIPPED stamping an
// out-of-range mtime, leaving the promoted file with its ~now write timestamp. The cheap-scan
// (size, mtime) hint (built from LocalMTime) then never matched the on-disk mtime, so scanView
// re-opened and MD4-re-hashed the file on every scan forever - a pure CPU/IO waste that never
// self-healed. The fix derives LocalMTime from the actual on-disk file whenever the wire mtime
// falls outside the SetMeta-applicable range (the mtime mirror of markSynced's hash/size
// derive-from-disk fallbacks).
//
// A later fix closes the same class on the IN-RANGE branch, which was recorded verbatim on
// the assumption "what Chtimes wrote is what a stat reads back": a filesystem storing coarser
// timestamps (FAT/FAT32's two seconds, a rounding network or FUSE mount) holds something else, so
// the hint missed forever there too - just as silently. LocalMTime is now always the value the
// promote observed the filesystem store, read off the sealed staging temp (stagedOnDiskMTime),
// which subsumes the out-of-range branch above and the zero-mtime one below.

// outOfRangeMtime is a millisecond-scale value past maxMtimeSeconds (4e9), exactly what a
// non-conformant DSM reporting a ms mtime as seconds lands past; SetMeta warns and skips it.
const outOfRangeMtime int64 = 5_000_000_000

// sameLenDiff returns a byte slice of the SAME length as b but different content, so a cheap
// scan's size-axis hint check still passes and only the (mtime-gated) content reuse is probed.
func sameLenDiff(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range out {
		out[i] = b[i] ^ 0xFF
	}
	return out
}

// cheapScanReusesHint runs a cheap scan (loadScanHints + scanView) after swapping the on-disk
// file at rel for the given same-size bytes at the file's current mtime, and reports whether the
// returned entry hash is the stored baseline hash (hint matched → no re-hash) rather than the
// hash of the swapped bytes (hint missed → re-hashed). It is the functional witness that the
// baseline's LocalMTime agrees with the on-disk mtime.
func cheapScanReusesHint(t *testing.T, e *Engine, viewID uint64, viewDir, rel string, swapped []byte, wantBaselineHash string) bool {
	t.Helper()
	p := filepath.Join(viewDir, rel)
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat %s: %v", p, err)
	}
	if err := os.WriteFile(p, swapped, 0o644); err != nil {
		t.Fatalf("swap %s: %v", p, err)
	}
	// Preserve the on-disk mtime exactly so ONLY the content changed under the same (size, mtime).
	if err := os.Chtimes(p, fi.ModTime(), fi.ModTime()); err != nil {
		t.Fatalf("chtimes %s: %v", p, err)
	}
	hints, err := e.loadScanHints(viewID)
	if err != nil {
		t.Fatalf("loadScanHints: %v", err)
	}
	entries, _, err := e.scanView(context.Background(), viewID, hints)
	if err != nil {
		t.Fatalf("scanView: %v", err)
	}
	en, ok := findEntry(entries, rel)
	if !ok {
		t.Fatalf("%s not scanned", rel)
	}
	if en.md4 == md4.ContentHash(swapped) {
		return false // re-hashed the swapped bytes → hint missed
	}
	if en.md4 != wantBaselineHash {
		t.Fatalf("cheap scan md4 = %q, want either reused %q or swapped %q", en.md4, wantBaselineHash, md4.ContentHash(swapped))
	}
	return true // reused the stored hash → hint matched
}

// TestInboundDownloadOutOfRangeMtimeDerivesFromDisk: a server modify whose reply carries an
// out-of-range mtime lands cleanly (SetMeta skips the mtime stamp), and after the fix the baseline
// records the file's actual on-disk mtime - not the raw wire value - so the cheap scan reuses the
// hint instead of re-hashing forever. Before the fix LocalMTime == outOfRangeMtime and the cheap
// scan re-hashes on every pass.
func TestInboundDownloadOutOfRangeMtimeDerivesFromDisk(t *testing.T) {
	oldContent := []byte("old-synced-content")
	newContent := []byte("new-server-content") // same length so a same-size swap can probe the hint
	newHash := md4.ContentHash(newContent)
	c := &pushClient{
		downloadContent: map[string][]byte{"fa": newContent},
		downloadNode: map[string]actions.Node{"fa": {
			FileID: "fa", Hash: newHash, Size: int64(len(newContent)), MTime: outOfRangeMtime,
		}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	p := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(p, oldContent, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h-old", LocalHash: md4.ContentHash(oldContent),
		LocalSize: int64(len(oldContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{
		SyncID: 6, Action: "modify", FileID: "fa", Path: "a.txt",
		Hash: newHash, Size: int64(len(newContent)), MTime: outOfRangeMtime,
	}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The promote wrote the new content but SetMeta skipped the out-of-range mtime, so the file
	// carries a real recent timestamp, well inside range.
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	onDisk := fi.ModTime().Unix()
	if fsutil.MTimeOutOfRange(onDisk) {
		t.Fatalf("test setup: on-disk mtime %d unexpectedly out of range", onDisk)
	}

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalMTime != onDisk {
		t.Errorf("LocalMTime = %d, want the on-disk mtime %d (raw wire %d would re-hash forever)", b.LocalMTime, onDisk, outOfRangeMtime)
	}
	if b.LocalMTime == outOfRangeMtime {
		t.Errorf("LocalMTime recorded the raw out-of-range wire mtime %d", outOfRangeMtime)
	}
	// ServerMTime keeps the raw wire value (it records what the server reported, unused as a hint).
	if b.ServerMTime != outOfRangeMtime {
		t.Errorf("ServerMTime = %d, want the raw wire value %d", b.ServerMTime, outOfRangeMtime)
	}

	// Functional witness: a same-size content swap at the SAME mtime is NOT noticed by a cheap
	// scan - proof the hint matches on-disk and the file is not re-hashed.
	if !cheapScanReusesHint(t, e, 7, viewDir, "a.txt", sameLenDiff(newContent), newHash) {
		t.Error("cheap scan re-hashed the file; the LocalMTime hint does not match the on-disk mtime")
	}
}

// TestInboundDownloadInRangeMtimeRecordedVerbatim is the regression: on a whole-second filesystem
// (NTFS/ext4/APFS - every host this runs on) an in-range wire mtime is stamped to disk and read
// back as itself, so the recorded LocalMTime equals it and the cheap scan matches. Both fixes
// change nothing for the common case; what the baseline holds is the on-disk value either way.
func TestInboundDownloadInRangeMtimeRecordedVerbatim(t *testing.T) {
	const inRangeMtime int64 = 1_700_000_000
	oldContent := []byte("old-synced-content")
	newContent := []byte("new-server-content")
	newHash := md4.ContentHash(newContent)
	c := &pushClient{
		downloadContent: map[string][]byte{"fa": newContent},
		downloadNode: map[string]actions.Node{"fa": {
			FileID: "fa", Hash: newHash, Size: int64(len(newContent)), MTime: inRangeMtime,
		}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	p := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(p, oldContent, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h-old", LocalHash: md4.ContentHash(oldContent),
		LocalSize: int64(len(oldContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{
		SyncID: 6, Action: "modify", FileID: "fa", Path: "a.txt",
		Hash: newHash, Size: int64(len(newContent)), MTime: inRangeMtime,
	}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// SetMeta stamped the in-range mtime onto the file.
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.ModTime().Unix() != inRangeMtime {
		t.Errorf("on-disk mtime = %d, want the stamped in-range value %d", fi.ModTime().Unix(), inRangeMtime)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalMTime != inRangeMtime {
		t.Errorf("LocalMTime = %d, want the wire value %d recorded verbatim", b.LocalMTime, inRangeMtime)
	}
	if !cheapScanReusesHint(t, e, 7, viewDir, "a.txt", sameLenDiff(newContent), newHash) {
		t.Error("cheap scan re-hashed an in-range-mtime file; the hint should match")
	}
}

// TestInboundDownloadCoarseMTimeMatchesHint pins the in-range fix: on a filesystem whose timestamp
// resolution is coarser than a second (FAT/FAT32's two, a rounding network or FUSE mount), an
// IN-RANGE wire mtime is not what the disk ends up holding either - so recording it verbatim built
// a cheap-scan hint that never matched, and every scan re-opened and MD4-re-hashed the file
// forever, silently and without self-healing (the content hash agrees, so nothing rewrites the
// baseline). After the fix LocalMTime is the value the filesystem stored, so the second scan
// reuses the hint; ServerMTime still records the raw wire value.
func TestInboundDownloadCoarseMTimeMatchesHint(t *testing.T) {
	// Odd second: unstorable on the modelled filesystem, so the wire value and the on-disk value
	// provably differ.
	const wireMtime int64 = 1_700_000_001
	oldContent := []byte("old-synced-content")
	newContent := []byte("new-server-content")
	newHash := md4.ContentHash(newContent)
	c := &pushClient{
		downloadContent: map[string][]byte{"fa": newContent},
		downloadNode: map[string]actions.Node{"fa": {
			FileID: "fa", Hash: newHash, Size: int64(len(newContent)), MTime: wireMtime,
		}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	coarseMTimeFS(t, e)

	p := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(p, oldContent, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "fa", FileType: "file", ServerHash: "h-old", LocalHash: md4.ContentHash(oldContent),
		LocalSize: int64(len(oldContent)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}

	events := []actions.Event{{
		SyncID: 6, Action: "modify", FileID: "fa", Path: "a.txt",
		Hash: newHash, Size: int64(len(newContent)), MTime: wireMtime,
	}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	onDisk := fi.ModTime().Unix()
	if onDisk == wireMtime {
		t.Fatalf("test setup: on-disk mtime %d equals the wire value; the coarse filesystem model did not apply", onDisk)
	}

	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalMTime != onDisk {
		t.Errorf("LocalMTime = %d, want the on-disk mtime %d (the wire value %d would re-hash forever)", b.LocalMTime, onDisk, wireMtime)
	}
	// ServerMTime is the server's report, not an on-disk fact: it keeps the unrounded wire value.
	if b.ServerMTime != wireMtime {
		t.Errorf("ServerMTime = %d, want the raw wire value %d", b.ServerMTime, wireMtime)
	}
	if !cheapScanReusesHint(t, e, 7, viewDir, "a.txt", sameLenDiff(newContent), newHash) {
		t.Error("cheap scan re-hashed the file; the LocalMTime hint does not match the coarse on-disk mtime")
	}
}

// TestStagedOnDiskMTime pins the helper every downloaded LocalMTime now comes from,
// including the three branches the end-to-end tests don't reach:
// a zero (sparse/unset) wire mtime - SetMeta leaves the modification time untouched, so the value
// on disk is the temp's write timestamp and NOT 0, which is what closes the last never-matching
// hint (a verbatim 0 matched nothing but the epoch); a coarse filesystem's rounding, seen through
// the stamp seam; and a stat failure, surfaced as an error that makes the apply
// transient-retry rather than a silently-wrong LocalMTime.
func TestStagedOnDiskMTime(t *testing.T) {
	// A bare Engine: the helper reads nothing but its own stamp seam, which is unset here
	// and so falls through to the real fsutil.SetMeta.
	e := &Engine{}
	dir := t.TempDir()
	write := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}
	diskMTime := func(p string) int64 {
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("lstat %s: %v", p, err)
		}
		return fi.ModTime().Unix()
	}

	// In-range value on a whole-second filesystem: stamped, and read back as itself.
	const inRange int64 = 1_700_000_000
	p := write("in-range")
	got, err := e.stagedOnDiskMTime(p, inRange)
	if err != nil || got != inRange {
		t.Errorf("in-range: got (%d, %v), want (%d, nil)", got, err, inRange)
	}
	if d := diskMTime(p); d != inRange {
		t.Errorf("in-range: on-disk mtime = %d, want the stamped %d", d, inRange)
	}

	// Zero (sparse/unset): the stamp is skipped, so the reported value is the write timestamp
	// already on disk - never 0, which no live file's mtime ever equals.
	p = write("zero")
	want := diskMTime(p)
	if got, err := e.stagedOnDiskMTime(p, 0); err != nil || got != want {
		t.Errorf("zero: got (%d, %v), want (%d, nil) - the temp's own write timestamp", got, err, want)
	}

	// Out of range: fsutil.SetMeta warns and skips, same outcome as the zero case.
	p = write("out-of-range")
	want = diskMTime(p)
	if got, err := e.stagedOnDiskMTime(p, outOfRangeMtime); err != nil || got != want {
		t.Errorf("out-of-range: got (%d, %v), want (%d, nil)", got, err, want)
	}

	// Coarse filesystem: the value asked for is not the value stored, and the helper reports what
	// was STORED (this is the whole point - the caller records a hint that matches).
	coarseMTimeFS(t, e)
	p = write("coarse")
	const odd int64 = 1_700_000_001
	got, err = e.stagedOnDiskMTime(p, odd)
	if err != nil {
		t.Fatalf("coarse: %v", err)
	}
	if got == odd {
		t.Errorf("coarse: got the wire value %d back; the modelled filesystem cannot store it", odd)
	}
	if d := diskMTime(p); got != d {
		t.Errorf("coarse: reported %d, on disk %d - the helper must report what the filesystem stored", got, d)
	}

	// A missing file: an error, not a silently-wrong mtime.
	if _, err := e.stagedOnDiskMTime(filepath.Join(dir, "gone"), inRange); err == nil {
		t.Error("missing file: got nil error, want a failure")
	}
}

// TestStagedOnDiskMTimeSkipsUnstampableValues pins that the staging stamp is not issued at all
// for a wire mtime SetMeta would refuse. Both refusal shapes already made the call a no-op on
// disk, so the observable cost was the operator-facing one: fsutil.SetMeta warns
// unconditionally for an out-of-range value, so the promote emitted the SAME warning TWICE per
// download - the first naming a `.dl-` staging temp that is deleted moments later and can never
// be inspected. The post-promote SetMeta still reports it against the real path, so nothing is
// silenced, only de-duplicated.
func TestStagedOnDiskMTimeSkipsUnstampableValues(t *testing.T) {
	dir := t.TempDir()
	var stamped []int64
	// A bare Engine with only the stamp seam set: stagedOnDiskMTime reads nothing else,
	// and a per-test engine means no process state is shared.
	e := &Engine{}
	e.setMeta = func(path string, mtime int64, unixPermDecimal uint32) error {
		stamped = append(stamped, mtime)
		return fsutil.SetMeta(path, mtime, unixPermDecimal)
	}

	for _, tc := range []struct {
		name  string
		mtime int64
		want  []int64
	}{
		{"out of range", outOfRangeMtime, nil},
		{"negative", -1, nil},
		{"zero", 0, nil},
		{"in range", 1_700_000_000, []int64{1_700_000_000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stamped = nil
			p := filepath.Join(dir, tc.name)
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := e.stagedOnDiskMTime(p, tc.mtime); err != nil {
				t.Fatalf("stagedOnDiskMTime: %v", err)
			}
			if len(stamped) != len(tc.want) {
				t.Fatalf("SetMeta calls = %v, want %v", stamped, tc.want)
			}
			for i := range tc.want {
				if stamped[i] != tc.want[i] {
					t.Errorf("SetMeta call %d stamped %d, want %d", i, stamped[i], tc.want[i])
				}
			}
		})
	}
}

// coarseMTimeGranularity is the two-second resolution FAT/FAT32 stores timestamps at; coarse
// network and FUSE mounts round similarly. It is deliberately larger than the whole second a
// baseline's LocalMTime records, so a truncated value is observably NOT the wire value.
const coarseMTimeGranularity int64 = 2

// coarseMTimeFS models such a filesystem for the duration of the test by routing the download
// promote's stamps through a SetMeta that truncates an in-range mtime to the granularity - the
// rounding a real coarse mount applies inside the filesystem, so it applies to every stamp of the
// promote (the temp's and the promoted path's), not only the one whose value is read back. The
// truncated value is really written, so the disk and the readback agree exactly as they would
// there; only the wire value is unstorable. No host this client is developed or tested on
// (NTFS/ext4/APFS) has coarser-than-second timestamps, so the seam is the only way to reach the
// branch.
func coarseMTimeFS(t *testing.T, e *Engine) {
	t.Helper()
	e.setMeta = func(path string, mtime int64, unixPermDecimal uint32) error {
		if mtime != 0 && !fsutil.MTimeOutOfRange(mtime) {
			mtime -= mtime % coarseMTimeGranularity
		}
		return fsutil.SetMeta(path, mtime, unixPermDecimal)
	}
}

// TestResolveConflictOutOfRangeMtimeDerivesFromDisk is the keep-both resolver mirror of the
// out-of-range fix (conflict.go downloadCanonical): a server node whose reply carries an
// out-of-range mtime is downloaded onto the canonical path (SetMeta skips the mtime stamp), and the
// resolved baseline must record the file's actual on-disk mtime - not the raw wire value - so the
// cheap scan reuses the hint instead of re-hashing the resolved file forever.
func TestResolveConflictOutOfRangeMtimeDerivesFromDisk(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("server-version-x") // same length as localBytes below for the hint probe
	localBytes := []byte("local-version-yy")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	serverNode := actions.Node{FileID: "srv1", Hash: serverHash, MTime: outOfRangeMtime, Size: int64(len(serverBytes))}
	c := &pushClient{
		queryNode:       map[string]actions.Node{"a.txt": serverNode},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": serverNode},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	canonical := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(canonical, localBytes, 0o644); err != nil {
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

	if got, err := os.ReadFile(canonical); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server bytes %q", got, err, serverBytes)
	}
	fi, err := os.Stat(canonical)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	onDisk := fi.ModTime().Unix()
	if fsutil.MTimeOutOfRange(onDisk) {
		t.Fatalf("test setup: on-disk mtime %d unexpectedly out of range", onDisk)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Fatalf("canonical baseline state = %q, want synced", b.State)
	}
	if b.LocalMTime != onDisk {
		t.Errorf("canonical LocalMTime = %d, want the on-disk mtime %d (raw wire %d would re-hash forever)", b.LocalMTime, onDisk, outOfRangeMtime)
	}
	if !cheapScanReusesHint(t, e, 7, viewDir, "a.txt", sameLenDiff(serverBytes), serverHash) {
		t.Error("cheap scan re-hashed the resolved file; the LocalMTime hint does not match the on-disk mtime")
	}
}

// TestResolveConflictCoarseMTimeMatchesHint is the keep-both mirror of the in-range fix: the
// resolver's canonical download (conflict.go downloadCanonical) recorded the wire mtime verbatim
// too, so on a coarse-granularity mount every resolved file re-hashed on every scan forever.
// LocalMTime must be what the filesystem stored; ServerMTime keeps the raw wire value.
func TestResolveConflictCoarseMTimeMatchesHint(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	const wireMtime int64 = 1_700_000_001 // odd second: unstorable on the modelled filesystem

	serverBytes := []byte("server-version-x") // same length as localBytes below for the hint probe
	localBytes := []byte("local-version-yy")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)

	copyRel := conflictCopyName("a.txt", host, ts)
	copyName := filepath.Base(copyRel)

	serverNode := actions.Node{FileID: "srv1", Hash: serverHash, MTime: wireMtime, Size: int64(len(serverBytes))}
	c := &pushClient{
		queryNode:       map[string]actions.Node{"a.txt": serverNode},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode:    map[string]actions.Node{"srv1": serverNode},
		uploadNode:      map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	coarseMTimeFS(t, e)
	pinClock(e, host, ts)

	canonical := filepath.Join(viewDir, "a.txt")
	if err := os.WriteFile(canonical, localBytes, 0o644); err != nil {
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

	if got, err := os.ReadFile(canonical); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server bytes %q", got, err, serverBytes)
	}
	fi, err := os.Stat(canonical)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	onDisk := fi.ModTime().Unix()
	if onDisk == wireMtime {
		t.Fatalf("test setup: on-disk mtime %d equals the wire value; the coarse filesystem model did not apply", onDisk)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced {
		t.Fatalf("canonical baseline state = %q, want synced", b.State)
	}
	if b.LocalMTime != onDisk {
		t.Errorf("canonical LocalMTime = %d, want the on-disk mtime %d (the wire value %d would re-hash forever)", b.LocalMTime, onDisk, wireMtime)
	}
	if b.ServerMTime != wireMtime {
		t.Errorf("canonical ServerMTime = %d, want the raw wire value %d", b.ServerMTime, wireMtime)
	}
	if !cheapScanReusesHint(t, e, 7, viewDir, "a.txt", sameLenDiff(serverBytes), serverHash) {
		t.Error("cheap scan re-hashed the resolved file; the LocalMTime hint does not match the coarse on-disk mtime")
	}
}
