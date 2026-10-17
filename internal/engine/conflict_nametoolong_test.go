package engine

// Keep-both's terminal exit for a name the host filesystem cannot hold.
//
// conflictCopyNameN and stagingTempPattern budget every generated basename to
// maxCopyNameComponentBytes (255), which holds on ext4/APFS/NTFS - but NOT on a mount whose
// per-component limit is lower (eCryptfs, some FUSE/network mounts), where a perfectly valid
// source name still yields an unappliable copy or staging-temp name. applyOp routes
// OpResolveConflict through its own early return, ahead of the shared nameTooLong classifier
// the inbound verbs use, so before this fix such a resolve retried every
// drain forever and - via blockedKeys - serialised every overlapping op behind it.
//
// The host cannot be made to reject a 255-byte name here, so the rejection is injected at the two
// sites that generate a name during a resolution: preserveConflictCopy's no-replace rename
// (the fsRenameNoReplace seam) and promoteStagedDownload's staging temp (fsCreateStagingTemp).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// The three seams below live on the Engine, so each test sets them on ITS OWN engine and
// nothing is shared between tests. They used to be package-level vars, which meant every
// test that touched one mutated process-global state and had to run alone.

// stubRenameNoReplace swaps preserveConflictCopy's no-replace rename on e.
func stubRenameNoReplace(t *testing.T, e *Engine, fn func(oldpath, newpath string) error) {
	t.Helper()
	e.renameNoReplace = fn
}

// stubConflictCopyLstat swaps the keep-both rollback's on-disk probe on e.
func stubConflictCopyLstat(t *testing.T, e *Engine, fn func(string) (os.FileInfo, error)) {
	t.Helper()
	e.conflictCopyLstat = fn
}

// stubCreateStagingTemp swaps promoteStagedDownload's staging-temp constructor on e.
func stubCreateStagingTemp(t *testing.T, e *Engine, fn func(dir, pattern string) (*os.File, error)) {
	t.Helper()
	e.createStagingTemp = fn
}

// conflictFixture seeds the standard keep-both scenario used by the tests below: a divergent
// local file at "a.txt" whose server version the fake client serves, plus the queued resolve op.
// It returns the engine, store, view directory and the local bytes on disk.
func conflictFixture(t *testing.T, host string, ts time.Time) (*Engine, *store.Store, string, []byte) {
	t.Helper()
	serverBytes := []byte("server-version")
	localBytes := []byte("local-version-divergent")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)
	copyName := filepath.Base(conflictCopyName("a.txt", host, ts))

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
	return e, st, viewDir, localBytes
}

// TestResolveConflictCopyNameTooLongDeadLetters: the keep-both copy's aside-rename is rejected
// with ENAMETOOLONG (a mount whose per-component limit is below the 255-byte budget). Before the
// fix that plain error fell through applyResolveConflict's terminal list into the transient arm
// and the op retried every drain forever. It is now terminal: the op is dead-lettered with a
// durable trace and the baseline marked StateConflict, while the divergent local file - whose
// rename is exactly what failed - stays untouched on disk.
func TestResolveConflictCopyNameTooLongDeadLetters(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})

	// errDeadLettered is swallowed by drainBatch, so a terminal drop drains cleanly.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the unappliable name is retrying forever", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
	// Data-safe: the aside-rename is what failed, so the divergent local content is still where
	// it was - the drop only means the server version is not materialised.
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
	// preserveAndRecord pre-writes the provenance row BEFORE the aside-rename (so a crash in
	// between never parks content without a record). The rename is exactly what failed here, so
	// no _Conflict copy exists - the speculative row must be rolled back rather than left behind
	// claiming a copy that was never created, which would make `conflicts` permanently misleading.
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want none: no _Conflict copy was ever created", recs)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
}

// TestResolveConflictCopyFailureRestoresPriorConflictRecord: the rollback restores what the
// pre-write displaced instead of deleting blindly. An earlier resolution of the same canonical
// path already parked a _Conflict copy and recorded its provenance; a later resolve whose
// aside-rename the filesystem refuses must leave that record intact - the earlier copy is still
// on disk, so dropping its row would lose the provenance of real content.
//
// That on-disk copy is what the restore now turns on: a prior naming a copy
// that is provably absent is a crash-stranded speculative row and is dropped instead - see
// TestResolveConflictSpeculativePriorNotResurrected.
func TestResolveConflictCopyFailureRestoresPriorConflictRecord(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, _ := conflictFixture(t, host, ts)
	prior := store.Conflict{
		CanonicalPath: "a.txt",
		CopyPath:      "a_Conflict_oldhost_20260101_000000.txt",
		CopyFileID:    "oldcopy",
		DetectedAt:    1767225600,
	}
	if err := os.WriteFile(filepath.Join(viewDir, filepath.FromSlash(prior.CopyPath)),
		[]byte("the earlier resolution's parked content"), 0o644); err != nil {
		t.Fatalf("seed prior copy on disk: %v", err)
	}
	if err := st.PutConflict(7, prior); err != nil {
		t.Fatalf("seed prior conflict: %v", err)
	}
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0] != prior {
		t.Errorf("conflict records = %+v, want the untouched prior record %+v", recs, prior)
	}
}

// TestResolveConflictCopyDurabilityFailureKeepsProvenance is the other half of the rollback's
// precondition: a preserveConflictCopy error is NOT proof that no copy exists. fsutil.RenameNoReplace
// fsyncs the destination directory AFTER the move (syncRenameDirs) and propagates a genuine I/O
// failure from it, so the aside can have landed under the returned error. Rolling the provenance row
// back there would leave the divergent content parked on disk with no record - the exact class the
// pre-write closes, and unrecoverable because the retry sees the source gone and takes the
// false-conflict fast path. The row must survive, naming the landed copy.
func TestResolveConflictCopyDurabilityFailureKeepsProvenance(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	copyRel := conflictCopyName("a.txt", host, ts)
	// The move lands; only the durability flush behind it fails - RenameNoReplace's real shape.
	stubRenameNoReplace(t, e, func(oldpath, newpath string) error {
		if err := os.Rename(oldpath, newpath); err != nil {
			return err
		}
		return fmt.Errorf("fsutil: fsync %s: input/output error", filepath.Dir(newpath))
	})

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the transient durability error, got nil")
	}
	// The keep-both copy really is on disk, holding the only copy of the divergent content.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil ||
		string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the landed %q", got, err, localBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Errorf("conflict records = %+v, want provenance for the parked a.txt/%s", recs, copyRel)
	}
}

// TestResolveConflictUnrepresentableCopyNameRollsBackBlindly closes the gap the evidence probe
// left on exactly the mount the nameTooLong exit exists for. When the aside-rename is refused
// because the generated name is past the mount's per-component limit, an Lstat of that same name
// fails the same way - it is not IsNotExist, so a probe-only rule reads "unprovable" and CORRECTS
// the speculative row to name a _Conflict copy the filesystem can never hold, leaving `conflicts`
// permanently claiming parked content that is really still at its canonical path. The host
// rejection of the name is itself the proof, ahead of the probe; here the probe is stubbed
// inconclusive (the failing mount's EIO) so only that arm can carry the rollback.
func TestResolveConflictUnrepresentableCopyNameRollsBackBlindly(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})
	stubConflictCopyLstat(t, e, func(string) (os.FileInfo, error) {
		return nil, fmt.Errorf("lstat: %w", syscall.EIO)
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("conflict records = %+v, want none: the copy name cannot exist on this mount", recs)
	}
	// The terminal drop is unchanged, and the divergent content is still at its canonical path -
	// which is what makes the dropped provenance row correct rather than a loss of evidence.
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
}

// TestConflictCopyProvablyAbsent tables the rollback's evidence rule directly, including the two
// arms the end-to-end tests cannot reach on a healthy host: a probe that is inconclusive rather
// than negative, and the host's name rejection surfacing from the probe instead of the rename.
func TestConflictCopyProvablyAbsent(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "landed")
	if err := os.WriteFile(present, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	absent := filepath.Join(dir, "never-created")

	tooLong := fmt.Errorf("rename: %w", syscall.ENAMETOOLONG)
	ioErr := fmt.Errorf("fsync: %w", syscall.EIO)

	tests := []struct {
		name      string
		path      string
		renameErr error
		probe     func(string) (os.FileInfo, error)
		want      bool
	}{
		// The name is unusable here, so the move cannot have landed - proof that outranks the
		// probe, which on that mount would fail identically and read as unprovable.
		{"name rejected, probe inconclusive", absent, tooLong, func(string) (os.FileInfo, error) { return nil, ioErr }, true},
		{"name rejected, probe sees an entry", present, tooLong, os.Lstat, true},
		// Without that proof only a definitive negative counts.
		{"durability failure, copy absent", absent, ioErr, os.Lstat, true},
		{"durability failure, copy landed", present, ioErr, os.Lstat, false},
		{"durability failure, probe inconclusive", absent, ioErr, func(string) (os.FileInfo, error) { return nil, ioErr }, false},
		{"probe reports the name rejection", absent, ioErr, func(string) (os.FileInfo, error) { return nil, tooLong }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A bare Engine: this predicate reads nothing but its own probe seam, and
			// giving each sub-test its own means they no longer share process state.
			e := &Engine{conflictCopyLstat: tc.probe}
			if got := e.conflictCopyProvablyAbsent(tc.path, tc.renameErr); got != tc.want {
				t.Errorf("conflictCopyProvablyAbsent = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveConflictNameTooLongUnblocksOverlappingChain is the wedge half of the problem: an op
// that overlaps the resolver's path is serialised behind it, so a resolve stuck in transient retry
// starves the authoritative inbound remove of the same path forever. With the terminal exit the
// resolve leaves the queue in the same drain and the remove applies right behind it.
func TestResolveConflictNameTooLongUnblocksOverlappingChain(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, syscall.ENAMETOOLONG)
	})
	// The server's own delete of the same path, enqueued behind the resolve. Its captured base
	// matches the on-disk content, so the never-lose guard honours the delete rather than
	// preserving a copy (which would need the very rename that fails here).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt",
		BaseWasSynced: true, BaseLocalHash: md4.ContentHash(localBytes),
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the chain behind the resolve is wedged", n, err)
	}
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("Lstat a.txt err = %v, want IsNotExist: the inbound remove never applied", err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want exactly the resolve's trace", n, err)
	}
}

// TestResolveConflictStagingNameTooLongDeadLetters covers the second name-generating half: the
// copy is set aside successfully and the server version's staging temp is what the filesystem
// rejects. Same terminal outcome - and the preserved copy plus its provenance record stay on
// disk, so the divergent content is not lost by the drop.
func TestResolveConflictStagingNameTooLongDeadLetters(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	copyRel := conflictCopyName("a.txt", host, ts)
	stubCreateStagingTemp(t, e, func(_, pattern string) (*os.File, error) {
		return nil, fmt.Errorf("open %s: %w", pattern, syscall.ENAMETOOLONG)
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want 0: the unappliable staging name is retrying forever", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want 1 trace for the dropped resolve-conflict op", n, err)
	}
	// The divergent content survives the drop as the keep-both copy the next localScan uploads.
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil ||
		string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the preserved %q", got, err, localBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "a.txt" || recs[0].CopyPath != copyRel {
		t.Errorf("conflict records = %+v, want the preserved copy's provenance for a.txt/%s", recs, copyRel)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
}

// TestResolveConflictCopyRenamePermissionStaysTransient pins the conservatism of the new arm: a
// permission failure on the same rename is NOT a permanent name rejection (an antivirus lock, a
// transient parent ACL), so the op keeps its old transient outcome - queued, untraced, retried.
func TestResolveConflictCopyRenamePermissionStaysTransient(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	stubRenameNoReplace(t, e, func(_, newpath string) error {
		return fmt.Errorf("rename %s: %w", newpath, os.ErrPermission)
	})

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want the transient permission error, got nil")
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Errorf("PendingLen = %d (err %v), want 1 (op left queued to retry)", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0: a permission failure must not dead-letter", n, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != string(localBytes) {
		t.Fatalf("local content = %q (err %v), want untouched %q", got, err, localBytes)
	}
}

// TestResolveConflictKeepBothWithDefaultSeams guards the seams themselves: with both variables at
// their production values the healthy keep-both resolution still completes end to end (the copy is
// set aside and uploaded, the server version lands on the canonical path, nothing is dropped).
func TestResolveConflictKeepBothWithDefaultSeams(t *testing.T) {
	ts := time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	e, st, viewDir, localBytes := conflictFixture(t, host, ts)
	copyRel := conflictCopyName("a.txt", host, ts)

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "a.txt")); err != nil || string(got) != "server-version" {
		t.Fatalf("canonical content = %q (err %v), want the server version", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, filepath.FromSlash(copyRel))); err != nil ||
		string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want %q", got, err, localBytes)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0", n, err)
	}
	if n, err := st.DeadLetterLen(); err != nil || n != 0 {
		t.Errorf("DeadLetterLen = %d (err %v), want 0 for a healthy resolve", n, err)
	}
}
