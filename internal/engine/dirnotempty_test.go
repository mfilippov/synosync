package engine

// Tests for the directory-not-empty fix: applyOp's fs.ErrExist arm dead-lettered the TRANSIENT
// "directory not empty" refusal alongside the permanent occupant shape, because Go folds
// ENOTEMPTY/ERROR_DIR_NOT_EMPTY into the same sentinel. For an inbound remove that drop is
// irreversible in the worst way: the op's baselines go with it, the half-deleted tree is left
// untracked on disk, and the next scan re-uploads it - resurrecting a server-deleted directory
// on every replica.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/fsutil"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// stubRemoveNode swaps the inbound delete seam (inbound_remove.go) for the duration of a test.
func stubRemoveNode(t *testing.T, fn func(string) error) {
	t.Helper()
	prev := removeNode
	removeNode = fn
	t.Cleanup(func() { removeNode = prev })
}

// failOnceThenRemove returns a removeNode stub that reports err for its first call and performs
// the real delete afterwards - the shape of a lost RemoveAll race: the blocker is gone by the
// next drain.
func failOnceThenRemove(err error) func(string) error {
	var once sync.Once
	return func(path string) error {
		first := false
		once.Do(func() { first = true })
		if first {
			return err
		}
		return fsutil.Remove(path)
	}
}

// enotempty builds the error os.RemoveAll reports when an entry appears between its readdir
// pass and the final rmdir, in the *fs.PathError wrapper the real call produces.
func enotempty(path string) error {
	return &fs.PathError{Op: "unlinkat", Path: path, Err: syscall.ENOTEMPTY}
}

// TestDirNotEmptyClassifiesRemovalRefusal pins the premise the verb gate rests on: the
// directory-not-empty refusal and the permanent-occupant refusal are the SAME sentinel to
// errors.Is (fs.ErrExist) and can only be told apart by errno.
func TestDirNotEmptyClassifiesRemovalRefusal(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantExist  bool // indistinguishable from an occupant through fs.ErrExist alone
		wantNotEmp bool
	}{
		{"ENOTEMPTY", enotempty("d"), true, true},
		{"bare ENOTEMPTY", syscall.ENOTEMPTY, true, true},
		{"EEXIST occupant", &fs.PathError{Op: "rename", Path: "d", Err: syscall.EEXIST}, true, false},
		{"fs.ErrExist itself", fs.ErrExist, true, false},
		{"ENOTDIR", &fs.PathError{Op: "mkdir", Path: "d", Err: syscall.ENOTDIR}, false, false},
		{"unrelated", errors.New("boom"), false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(tc.err, fs.ErrExist); got != tc.wantExist {
				t.Errorf("errors.Is(%v, fs.ErrExist) = %v, want %v", tc.err, got, tc.wantExist)
			}
			if got := dirNotEmpty(tc.err); got != tc.wantNotEmp {
				t.Errorf("dirNotEmpty(%v) = %v, want %v", tc.err, got, tc.wantNotEmp)
			}
		})
	}
}

// TestDirNotEmptyClassifiesHostNativeCode drives the classifier with the code THIS host really
// reports, not the syscall.ENOTEMPTY the seam-driven fixtures inject. Without it the Windows arm
// of the fix (extraDirNotEmptyErrors = ERROR_DIR_NOT_EMPTY) is untestable: every other test in
// this file injects ENOTEMPTY, which the shared check matches on every platform, so deleting the
// per-OS list would leave the whole suite green while a real lost RemoveAll race dead-lettered
// the OpRemove and resurrected a server-deleted subtree. The mirror of
// TestIsDiskFullClassifiesHostErrors (diskfull_test.go).
func TestDirNotEmptyClassifiesHostNativeCode(t *testing.T) {
	for _, tc := range hostDirNotEmptyErrs {
		t.Run(tc.name, func(t *testing.T) {
			if !dirNotEmpty(tc.err) {
				t.Errorf("dirNotEmpty(%v) = false, want the host's directory-not-empty code classified", tc.err)
			}
			wrapped := &fs.PathError{Op: "remove", Path: "d", Err: tc.err}
			if !dirNotEmpty(wrapped) {
				t.Errorf("dirNotEmpty(%v) = false through the *fs.PathError the real call wraps it in", wrapped)
			}
		})
	}
	for _, tc := range hostNotDirNotEmptyErrs {
		t.Run("not/"+tc.name, func(t *testing.T) {
			if dirNotEmpty(tc.err) {
				t.Errorf("dirNotEmpty(%v) = true, want false: over-claiming turns a permanent occupant into an endless retry", tc.err)
			}
		})
	}
}

// TestHostNativeDirNotEmptyDrivesTheVerbGate closes the loop: the host's own code must reach
// existsBlocksForever's transient arm for the two removing verbs and its permanent arm for the
// placing ones. On Windows this is the only test that fails if extraDirNotEmptyErrors loses
// ERROR_DIR_NOT_EMPTY - the code does satisfy errors.Is(err, fs.ErrExist), so the op would be
// dead-lettered with its baselines instead of retried.
func TestHostNativeDirNotEmptyDrivesTheVerbGate(t *testing.T) {
	err := &fs.PathError{Op: "remove", Path: "d", Err: hostNativeDirNotEmptyErr}
	if runtime.GOOS == "windows" && errors.Is(hostNativeDirNotEmptyErr, syscall.ENOTEMPTY) {
		t.Fatalf("hostNativeDirNotEmptyErr (%v) matches syscall.ENOTEMPTY on windows: pick a code the shared check misses, or the per-OS list is not what is under test",
			hostNativeDirNotEmptyErr)
	}
	if !errors.Is(err, fs.ErrExist) {
		t.Fatalf("errors.Is(%v, fs.ErrExist) = false: the premise the verb gate exists for no longer holds on this host", err)
	}
	for _, verb := range []store.OpType{store.OpRemove, store.OpDownload} {
		if existsBlocksForever(store.Op{Type: verb}, err) {
			t.Errorf("existsBlocksForever(%s, %v) = true, want false (the lost RemoveAll race is transient)", verb, err)
		}
	}
	for _, verb := range []store.OpType{store.OpMkdir, store.OpRename, store.OpUpload} {
		if !existsBlocksForever(store.Op{Type: verb}, err) {
			t.Errorf("existsBlocksForever(%s, %v) = false, want true (a placing verb's occupant never clears)", verb, err)
		}
	}
}

// TestExistsBlocksForeverGatesByVerb is the classifier matrix: the ENOTEMPTY shape is permanent
// for the file/dir-PLACING verbs (whose occupant rationale is untouched) and transient for the
// two verbs whose apply removes a directory, while a genuine EEXIST stays permanent for every
// verb - the gate keys on the shape first, the verb second.
func TestExistsBlocksForeverGatesByVerb(t *testing.T) {
	verbs := []store.OpType{store.OpRemove, store.OpDownload, store.OpMkdir, store.OpRename}
	permanentForNotEmpty := map[store.OpType]bool{
		store.OpRemove:   false,
		store.OpDownload: false,
		store.OpMkdir:    true,
		store.OpRename:   true,
	}
	for _, verb := range verbs {
		op := store.Op{Type: verb}
		if got := existsBlocksForever(op, enotempty("d")); got != permanentForNotEmpty[verb] {
			t.Errorf("existsBlocksForever(%s, ENOTEMPTY) = %v, want %v", verb, got, permanentForNotEmpty[verb])
		}
		occupant := &fs.PathError{Op: "rename", Path: "d", Err: syscall.EEXIST}
		if !existsBlocksForever(op, occupant) {
			t.Errorf("existsBlocksForever(%s, EEXIST) = false, want true (a real occupant never clears)", verb)
		}
		if existsBlocksForever(op, errors.New("boom")) {
			t.Errorf("existsBlocksForever(%s, non-exist error) = true, want false", verb)
		}
	}
}

// removeRaceFixture seeds a tracked, fully in-sync directory `d` (one synced child) under an
// inbound delete for it - the shape a server-side directory delete produces - and returns the
// store, engine and view dir.
func removeRaceFixture(t *testing.T) (*store.Store, *Engine, string) {
	t.Helper()
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(filepath.Join(viewDir, "d"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	child := []byte("in-sync child")
	if err := os.WriteFile(filepath.Join(viewDir, "d", "a.txt"), child, 0o644); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	hash := md4.ContentHash(child)
	if err := st.PutBaseline(7, "d", store.Baseline{
		FileID: "fdir", FileType: "dir", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed dir baseline: %v", err)
	}
	// The child is tracked and matches on disk, so the pre-delete divergence sweep finds nothing
	// to preserve and the delete proceeds - isolating the removal errno as the only variable.
	if err := st.PutBaseline(7, "d/a.txt", store.Baseline{
		FileID: "fchild", FileType: "file", ServerHash: hash, LocalHash: hash,
		LocalSize: int64(len(child)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed child baseline: %v", err)
	}
	e := New(st, &concTransport{client: &dlClient{}}, root, "sess", nil)
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "d",
		FileID: "fdir", BaseWasSynced: true, BaseIsDir: true,
	}); err != nil {
		t.Fatalf("Enqueue remove: %v", err)
	}
	return st, e, viewDir
}

// TestInboundDirRemoveSurvivesRemoveAllRace is the red test of the race. A user application
// drops a file into the directory while os.RemoveAll is walking it, so the delete reports
// ENOTEMPTY. Before the verb gate the classifier read that as a permanent occupant: the op was
// dead-lettered and dropRejectedRemoveBaseline dropped the directory's baseline AND its
// descendants', leaving the half-deleted tree untracked - the next scan would re-upload it and
// resurrect the server-deleted directory. The op must stay queued instead, and the next drain
// (once the racing writer is done) must converge.
func TestInboundDirRemoveSurvivesRemoveAllRace(t *testing.T) {
	st, e, viewDir := removeRaceFixture(t)
	stubRemoveNode(t, failOnceThenRemove(enotempty(filepath.Join(viewDir, "d"))))

	err := e.drainQueue(context.Background())
	if err == nil || !errors.Is(err, fs.ErrExist) {
		t.Fatalf("first drain err = %v, want the transient ENOTEMPTY apply failure", err)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 1 {
		t.Fatalf("pending ops = %d (err %v), want the remove still queued", n, perr)
	}
	if n, derr := st.DeadLetterLen(); derr != nil || n != 0 {
		t.Fatalf("dead letters = %d (err %v), want 0: a lost RemoveAll race is not a permanent occupant", n, derr)
	}
	// The decisive never-resurrect assertions: the baselines that describe the tree must survive,
	// so the still-present on-disk remnant is never diffed as an untracked local create.
	if _, gerr := st.GetBaseline(7, "d"); gerr != nil {
		t.Fatalf("directory baseline dropped by a transient failure: %v", gerr)
	}
	if _, gerr := st.GetBaseline(7, "d/a.txt"); gerr != nil {
		t.Fatalf("descendant baseline dropped by a transient failure: %v", gerr)
	}

	// The blocker is gone; the very same op applies.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Fatalf("pending ops after the retry = %d (err %v), want 0", n, perr)
	}
	if _, serr := os.Lstat(filepath.Join(viewDir, "d")); !os.IsNotExist(serr) {
		t.Errorf("directory still on disk after the retry: %v", serr)
	}
	if _, gerr := st.GetBaseline(7, "d"); !isNotFound(gerr) {
		t.Errorf("directory baseline survived the applied delete: %v", gerr)
	}
	if _, gerr := st.GetBaseline(7, "d/a.txt"); !isNotFound(gerr) {
		t.Errorf("descendant baseline survived the applied delete: %v", gerr)
	}
}

// TestInboundRemoveGenuineExistStillDeadLetters is the other half of the gate: only the
// dir-not-empty SHAPE is transient. A remove that fails with a genuine EEXIST keeps the previous
// terminal classification, so the carve-out cannot be widened into "every fs.ErrExist on a
// remove retries forever". The promote-occupant arc it mirrors - a fresh download whose
// no-replace promote hits a real occupant - is pinned by
// TestDrainQueueDeadLettersDownloadOverLocalDir, which drives the error off the real filesystem.
func TestInboundRemoveGenuineExistStillDeadLetters(t *testing.T) {
	st, e, viewDir := removeRaceFixture(t)
	stubRemoveNode(t, func(string) error {
		return &fs.PathError{Op: "rename", Path: filepath.Join(viewDir, "d"), Err: syscall.EEXIST}
	})

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue should dead-letter a permanent EEXIST, not wedge: %v", err)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 0 {
		t.Fatalf("pending ops = %d (err %v), want 0 (dead-lettered)", n, perr)
	}
	if n, derr := st.DeadLetterLen(); derr != nil || n != 1 {
		t.Fatalf("dead letters = %d (err %v), want 1", n, derr)
	}
}

// TestDownloadPriorPathRemoveRaceStaysQueued is the mirror the round found by unfolding: the
// same ENOTEMPTY comes out of removeRelocatedPriorFile - the cleanup of a node's stale copy at
// its previous tracked path - and dead-lettered an op of an entirely DIFFERENT verb, the
// download, whose own target was never occupied.
func TestDownloadPriorPathRemoveRaceStaysQueued(t *testing.T) {
	origBytes := []byte("last-synced-content")
	serverBytes := []byte("server-content-at-new-path")
	origHash := md4.ContentHash(origBytes)
	serverHash := md4.ContentHash(serverBytes)

	c := &pushClient{
		downloadContent: map[string][]byte{"f1": serverBytes},
		downloadNode:    map[string]actions.Node{"f1": {FileID: "f1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	// f1 is tracked (synced, content matching its recorded hash) at old.txt, so the stale-copy
	// cleanup deletes it outright rather than preserving anything - again isolating the errno.
	if err := os.WriteFile(filepath.Join(viewDir, "old.txt"), origBytes, 0o644); err != nil {
		t.Fatalf("seed prior file: %v", err)
	}
	if err := st.PutBaseline(7, "old.txt", store.Baseline{
		FileID: "f1", FileType: "file", ServerHash: origHash, LocalHash: origHash,
		LocalSize: int64(len(origBytes)), State: store.StateSynced,
	}); err != nil {
		t.Fatalf("seed prior baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "new.txt", FileID: "f1", UnixPerm: 644,
		LocalHash: serverHash, LocalSize: int64(len(serverBytes)), LocalMTime: 555,
	}); err != nil {
		t.Fatalf("Enqueue download: %v", err)
	}
	stubRemoveNode(t, func(string) error { return enotempty(filepath.Join(viewDir, "old.txt")) })

	err := e.drainQueue(context.Background())
	if err == nil || !errors.Is(err, fs.ErrExist) {
		t.Fatalf("drain err = %v, want the transient ENOTEMPTY apply failure", err)
	}
	if n, perr := st.PendingLen(); perr != nil || n != 1 {
		t.Fatalf("pending ops = %d (err %v), want the download still queued", n, perr)
	}
	if n, derr := st.DeadLetterLen(); derr != nil || n != 0 {
		t.Fatalf("dead letters = %d (err %v), want 0: the download itself was never unappliable", n, derr)
	}
	// The node's record at its prior path is untouched, so the retry still knows what to clean up.
	if _, gerr := st.GetBaseline(7, "old.txt"); gerr != nil {
		t.Fatalf("prior-path baseline dropped by a transient failure: %v", gerr)
	}
}

// TestRemoveAllRaceReportsDirNotEmpty is the empirical pin of the primitive this fix rests
// on: a file created in a directory while os.RemoveAll walks it makes the delete report the
// host's "not empty" errno, which errors.Is reads as fs.ErrExist. The race is not schedulable on
// demand, so the test asserts only when it reproduces and skips otherwise - it can fail the
// build only by contradicting the premise, never by losing the race.
func TestRemoveAllRaceReportsDirNotEmpty(t *testing.T) {
	const attempts = 12
	for attempt := range attempts {
		dir := filepath.Join(t.TempDir(), "d")
		// Enough entries that the walk has a window a concurrent create can land in.
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		for i := range 400 {
			name := filepath.Join(dir, "seed"+string(rune('a'+i%26))+string(rune('a'+i/26)))
			if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
				t.Fatalf("seed entry: %v", err)
			}
		}
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				//nolint:errcheck // the write races the delete by design; either outcome is fine
				_ = os.WriteFile(filepath.Join(dir, "racer"+string(rune('a'+i%26))), []byte("x"), 0o644)
			}
		}()
		err := os.RemoveAll(dir)
		close(stop)
		<-done
		//nolint:errcheck // best-effort cleanup of whatever the racer left behind
		_ = os.RemoveAll(dir)
		if err == nil {
			continue
		}
		if !dirNotEmpty(err) {
			// Some other loser of the race (a create failing mid-walk); not this test's subject.
			t.Logf("attempt %d: RemoveAll failed with a non-ENOTEMPTY error: %v", attempt, err)
			continue
		}
		if !errors.Is(err, fs.ErrExist) {
			t.Fatalf("host reports %v, which errors.Is does NOT read as fs.ErrExist - "+
				"the classifier's premise no longer holds", err)
		}
		return
	}
	t.Skipf("os.RemoveAll never lost the create race in %d attempts on this host", attempts)
}
