package engine

// These tests cover an outbound Upload/Mkdir/Rename under a directory the server deleted:
// it must not wedge the queue forever. The ancestor-walk absence proof was first given only to
// the remove verb (applyRemoveOut); the create/move verbs had no equivalent exit, so in the
// "outbound op drains BEFORE the inbound OpRemove" order (Order A) the doomed op retried forever
// and - serialised ahead of the authoritative inbound OpRemove - blocked that remove's never-lose
// guardRemoveDir preserve too, while in the "OpRemove drained first" order (Order B) the local
// subtree was carried into a _Conflict copy (dropping the parent baseline) and the later op then
// wedged on the never-resolving errParentUnknown.
//
// The TestOutboundRenameSourceNode* tests at the bottom cover a sibling class: an
// outbound OpRename whose source NODE (not a parent directory) the server deleted or moved away
// while both parents stayed alive had no terminal exit at all - the parent probes read the
// parents present, nothing else in applyOutbound's matrix matches a bare ErrPathError, and the
// authoritative inbound op serialized behind the wedge never ran.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// soleConflictCopy returns the name of the ONE keep-both copy the never-lose funnel parked directly
// under viewDir, failing if there is any other number of them.
//
// These tests assert that the doomed op's local subtree survived whole in a copy; the copy's NAME
// embeds the clock reading of the pass that made it, which is not what they are about.
// That reading is no longer the pinned timestamp for a copy made on a later pass:
// the absence corroboration weighs wall clock as well as drain passes (absenceStreakMinAge), so
// drainSettled advances the engine's clock between passes. Locating the copy by shape keeps the
// assertion - exactly one copy, holding exactly the local content - without pinning the pass it
// landed in. conflictCopyName's own format stays pinned by the resolver tests that call it directly.
func soleConflictCopy(t *testing.T, viewDir string) string {
	t.Helper()
	entries, err := os.ReadDir(viewDir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", viewDir, err)
	}
	var found []string
	for _, en := range entries {
		if strings.Contains(en.Name(), "_Conflict") {
			found = append(found, en.Name())
		}
	}
	if len(found) != 1 {
		t.Fatalf("keep-both copies in %s = %v, want exactly one", viewDir, found)
	}
	return found[0]
}

// TestOutboundUploadUnderDeletedParentOrderA reproduces the HIGH queue wedge and pins the fix:
// a local edit inside a/ produced a queued OpUpload(a/x); the server then deleted a/, whose
// inbound OpRemove(a) is serialised BEHIND the upload. The upload's Upload → ErrPathError and its
// post-turn QueryNode probe → ErrPathError, so before the fix it retried forever and blocked the
// remove. With the ancestor-walk exit the upload dead-letters in one drain (its baseline dropped),
// the inbound remove then runs and guardRemoveDir preserves the edited file as a _Conflict copy.
func TestOutboundUploadUnderDeletedParentOrderA(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	edited := []byte("locally-edited-content-not-yet-on-server")

	c := &pushClient{
		uploadErr: actions.ErrPathError,
		// "a" itself answers the path-error sentinel: the deleted directory corroborates the
		// listing that omits it (a listing alone can be a page).
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError, "a": actions.ErrPathError},
		// The ancestor walk lists the deleted parent's PARENT (the view root); a clean listing
		// that omits "a" is positive proof the whole chain is gone.
		listResult: map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	// a is a synced directory; a/x is a modify pending its upload.
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(edited),
		LocalSize: int64(len(edited)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	// The outbound upload is enqueued FIRST, so it drains ahead of the inbound remove (Order A).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
		LocalHash: md4.ContentHash(edited), LocalSize: int64(len(edited)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	// The server delete of a/ reconciles into a serialised inbound OpRemove(a).
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	// Both ops left the queue: the upload dead-lettered via the ancestor walk, the remove applied.
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (upload wedged the queue before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl == 0 {
		t.Error("no dead-letter recorded for the doomed upload")
	}
	// The canonical directory is gone and the edited file survives in a _Conflict copy.
	if _, err := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("a survived as the canonical path: %v", err)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "x"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("preserved edit = %q (err %v), want %q", got, err, edited)
	}
}

// TestOutboundLargeUploadUnderDeletedParentOrderA is the large-file (resume) variant of Order A:
// the edited file exceeds UploadResumeThreshold, so applyUploadOut takes the chunked resume path
// whose ResumeGetToken against the stale parent id returns ErrPathError. That failure is NOT
// path/stale-base-probed (the size gate skips the small-file probe), so the parent-gone ancestor
// walk must still run for it - otherwise the upload returns a transient error and wedges the queue
// forever, blocking the inbound OpRemove(a) serialised behind it (the exact wedge the ancestor walk
// closed for small files but originally left open for files over the resume threshold).
func TestOutboundLargeUploadUnderDeletedParentOrderA(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	edited := []byte("locally-edited-large-content-not-yet-on-server")

	c := &pushClient{
		// The large resume path fails to even obtain a token: the parent id is gone server-side.
		resumeTokenErr: actions.ErrPathError,
		// The ancestor walk lists the deleted parent's PARENT (the view root); a clean listing
		// that omits "a" is positive proof the whole chain is gone - corroborated by "a" itself
		// answering the path-error sentinel.
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
		queryNodeErrByPath: map[string]error{"a": actions.ErrPathError},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	e.limits.UploadResumeThreshold = 16 // force the resume path for this small test file
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(edited),
		LocalSize: int64(len(edited)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	// The outbound upload is enqueued FIRST, so it drains ahead of the inbound remove (Order A).
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
		LocalHash: md4.ContentHash(edited), LocalSize: int64(len(edited)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	// Both ops left the queue: the large upload dead-lettered via the ancestor walk, the remove applied.
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (large upload wedged the queue before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl == 0 {
		t.Error("no dead-letter recorded for the doomed large upload")
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("a survived as the canonical path: %v", err)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "x"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("preserved edit = %q (err %v), want %q", got, err, edited)
	}
}

// TestOutboundMkdirUnderDeletedParentOrderA is the mkdir variant of Order A: a locally created
// directory a/new produced OpMkdir(a/new) while the server deleted a/. The Mkdir → ErrPathError
// and its QueryNode replay probe → ErrPathError, so before the fix the mkdir wedged; the
// ancestor walk now drops it and the inbound remove preserves the a/ subtree.
func TestOutboundMkdirUnderDeletedParentOrderA(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	c := &pushClient{
		mkdirErr: actions.ErrPathError,
		// "a" answers the path-error sentinel too, corroborating the root listing that omits it.
		queryNodeErrByPath: map[string]error{"a/new": actions.ErrPathError, "a": actions.ErrPathError},
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a", "new"), 0o755); err != nil {
		t.Fatalf("seed dirs: %v", err)
	}
	// An untracked file inside the new directory gives the subtree content to preserve.
	child := []byte("new-dir-child-the-server-never-saw")
	if err := os.WriteFile(filepath.Join(viewDir, "a", "new", "f.txt"), child, 0o644); err != nil {
		t.Fatalf("seed new-dir child: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/new", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed a/new baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "a/new",
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (mkdir wedged the queue before the fix)", n)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("a survived as the canonical path: %v", err)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "new", "f.txt"))
	if err != nil || string(got) != string(child) {
		t.Fatalf("preserved new-dir child = %q (err %v), want %q", got, err, child)
	}
}

// TestOutboundRenameUnderDeletedParentOrderA is the rename variant of Order A: a local move
// a/x → a/y produced OpRename while the server deleted a/. A rename addresses both endpoints by
// path (not by parent id), so this same server-turn ancestor-walk exit covers both drain orders.
func TestOutboundRenameUnderDeletedParentOrderA(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr: actions.ErrPathError,
		// "a" answers the path-error sentinel too, corroborating the root listing that omits it.
		queryNodeErrByPath: map[string]error{"a/y": actions.ErrPathError, "a": actions.ErrPathError},
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	// The move already happened locally: a/y holds the file, a/x is gone.
	if err := os.WriteFile(filepath.Join(viewDir, "a", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// localReconcile already relocated the baseline to the destination (StatePending).
	if err := st.PutBaseline(7, "a/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "a/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (rename wedged the queue before the fix)", n)
	}
	if _, err := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("a survived as the canonical path: %v", err)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("preserved moved file = %q (err %v), want %q", got, err, moved)
	}
}

// TestOutboundRenameSourceParentDeletedOrderA covers the endpoint the destination-only probe
// missed: a CROSS-directory move a/x → b/y produced OpRename while the server deleted the
// SOURCE's parent a/ (the destination parent b/ stays live). pc.Rename fails ErrPathError
// because the source is gone, but the destination's parent b/ is present, so probing only
// op.NewPath (as the pre-fix gate did) never confirmed absence and the op wedged the queue
// forever - and, serialised ahead of the inbound OpRemove(a), blocked that authoritative
// remove too. Probing the SOURCE endpoint as well dead-letters the rename in one drain; the
// moved file survives at b/y untracked (baseline dropped) so the next scan re-uploads it into
// the still-live b/, and the emptied source directory is removed by the inbound OpRemove(a).
func TestOutboundRenameSourceParentDeletedOrderA(t *testing.T) {
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr: actions.ErrPathError,
		// a/ is deleted, so its own path answers the sentinel and corroborates the root listing
		// that omits it; b/ is alive and never probed for absence.
		queryNodeErrByPath: map[string]error{"b/y": actions.ErrPathError, "a": actions.ErrPathError},
		// The root listing has b/ (destination parent alive) but not a/ (deleted with the
		// source): the ancestor walk lists the view root for both endpoint probes, so b/ reads
		// present (destination probe → not gone) and a/ reads absent (source probe → gone).
		listResult: map[string][]actions.Node{"": {{Name: "b", FileID: "fb"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir a: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(viewDir, "b"), 0o755); err != nil {
		t.Fatalf("seed dir b: %v", err)
	}
	// The move already happened locally: b/y holds the file, a/x is gone.
	if err := os.WriteFile(filepath.Join(viewDir, "b", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b", store.Baseline{FileID: "fb", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed b baseline: %v", err)
	}
	// localReconcile already relocated the baseline to the destination (StatePending).
	if err := st.PutBaseline(7, "b/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed b/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "b/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// The server delete of a/ reconciles into a serialised inbound OpRemove(a).
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (the source-parent-gone rename wedged the queue before the fix)", n)
	}
	if dl, _ := st.DeadLetterLen(); dl == 0 {
		t.Error("no dead-letter recorded for the doomed rename")
	}
	// The moved file survives at the live destination, untracked so the next scan re-uploads it.
	got, err := os.ReadFile(filepath.Join(viewDir, "b", "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("moved file at b/y = %q (err %v), want %q", got, err, moved)
	}
	if b, gerr := st.GetBaseline(7, "b/y"); !isNotFound(gerr) {
		t.Errorf("b/y baseline not dropped: %+v (err %v)", b, gerr)
	}
	// The emptied source directory was removed by the trailing inbound OpRemove(a).
	if _, serr := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(serr) {
		t.Errorf("source dir a survived: %v", serr)
	}
}

// TestOutboundUploadUnderDeletedGrandparentOrderA covers the child of
// a NOT-YET-PUSHED mkdir under a server-deleted grandparent. The user created a/new/ and
// a/new/f.txt (OpMkdir(a/new) + OpUpload(a/new/f.txt), both id-less), then the server deleted
// a/ (inbound OpRemove(a) serialized last). The mkdir reaches its server turn, fails
// ErrPathError and dead-letters via the ancestor walk - and its cleanup DROPS the
// a/new baseline. The upload, draining next in the same batch, then dies in resolveParentID
// (the parent baseline was just deleted → errParentUnknown) with its source still on disk, so
// before the fix it stayed transient forever and the authoritative OpRemove(a) - with its
// never-lose preserve gate - skipped behind it every drain: a permanent cyclic wedge. With the
// pre-turn ancestor-walk exit the upload dead-letters in the same drain and the remove
// preserves the whole new/ subtree as a _Conflict copy.
func TestOutboundUploadUnderDeletedGrandparentOrderA(t *testing.T) {
	ts := time.Date(2026, 7, 17, 11, 20, 3, 0, time.UTC)
	const host = "laptop"
	child := []byte("new-subtree-child-the-server-never-saw")

	c := &pushClient{
		mkdirErr: actions.ErrPathError,
		// Every deleted path answers the sentinel, so each rung of the walk corroborates its
		// listing-derived verdict with a probe of its own path.
		queryNodeErrByPath: map[string]error{"a/new": actions.ErrPathError, "a": actions.ErrPathError},
		// Listing the deleted a/ itself errors; the walk then climbs to the view root, whose
		// clean listing omits "a" - positive proof the whole chain is gone.
		listErrByPath: map[string]error{"a": actions.ErrPathError},
		listResult:    map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a", "new"), 0o755); err != nil {
		t.Fatalf("seed dirs: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "new", "f.txt"), child, 0o644); err != nil {
		t.Fatalf("seed new-dir child: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	// The scan already planned the creates: parent-before-child, both baselines pending.
	if err := st.PutBaseline(7, "a/new", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed a/new baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/new/f.txt", store.Baseline{
		FileType: "file", LocalHash: md4.ContentHash(child),
		LocalSize: int64(len(child)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/new/f.txt baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "a/new",
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/new/f.txt",
		LocalHash: md4.ContentHash(child), LocalSize: int64(len(child)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	// The server delete of a/ reconciles into a serialized inbound OpRemove(a), last in line.
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want all three ops to converge within the corroboration passes; upload wedged before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (upload wedged the queue before the fix)", n)
	}
	// Both doomed creates left durable traces (mkdir via the server-turn walk, the upload via
	// the pre-turn walk).
	if dl, _ := st.DeadLetterLen(); dl != 2 {
		t.Errorf("dead-letter traces = %d, want 2 (mkdir + upload)", dl)
	}
	// The canonical directory is gone and the never-pushed child survives in a _Conflict copy.
	if _, err := os.Stat(filepath.Join(viewDir, "a")); !os.IsNotExist(err) {
		t.Errorf("a survived as the canonical path: %v", err)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "new", "f.txt"))
	if err != nil || string(got) != string(child) {
		t.Fatalf("preserved new-dir child = %q (err %v), want %q", got, err, child)
	}
}

// TestOutboundUploadUnderDeletedParentOrderB reproduces the OTHER drain order: the inbound
// OpRemove(a) drains first - preserveAside carries the whole subtree (with the edited file) into
// a _Conflict copy and drops the parent baseline - and the OpUpload then finds no parent baseline.
// Before the fix resolveParentID returned errParentUnknown and the upload retried forever; the
// source-gone probe now routes it through handleVanishedUpload, which drops it cleanly.
func TestOutboundUploadUnderDeletedParentOrderB(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"
	edited := []byte("locally-edited-content-order-b")

	c := &pushClient{
		// Both deleted paths answer the sentinel, corroborating the listings that omit them.
		queryNodeErrByPath: map[string]error{"a/x": actions.ErrPathError, "a": actions.ErrPathError},
		listErrByPath:      map[string]error{"a": actions.ErrPathError},
		listResult:         map[string][]actions.Node{"": {{Name: "other.txt", FileID: "fo"}}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "a", "x"), edited, 0o644); err != nil {
		t.Fatalf("seed edited file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/x", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(edited),
		LocalSize: int64(len(edited)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed a/x baseline: %v", err)
	}
	// The inbound remove is enqueued FIRST (lower seq), so it drains ahead of the upload (Order B).
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a/x", FileID: "fx",
		LocalHash: md4.ContentHash(edited), LocalSize: int64(len(edited)),
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge within the corroboration passes)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (upload wedged on errParentUnknown before the fix)", n)
	}
	// The remove preserved the subtree; the edited content is safe in the _Conflict copy.
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "x"))
	if err != nil || string(got) != string(edited) {
		t.Fatalf("preserved edit = %q (err %v), want %q", got, err, edited)
	}
}

// TestOutboundMkdirUnderDeletedParentOrderB is the mkdir Order B: the remove ran first and carried
// a/ (with a/new) into a _Conflict copy, so OpMkdir(a/new) finds neither a parent baseline nor the
// local directory at its canonical path. The local-target-gone probe drops it terminally instead
// of wedging on errParentUnknown.
func TestOutboundMkdirUnderDeletedParentOrderB(t *testing.T) {
	ts := time.Date(2026, 7, 16, 15, 30, 4, 0, time.UTC)
	const host = "laptop"

	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.MkdirAll(filepath.Join(viewDir, "a", "new"), 0o755); err != nil {
		t.Fatalf("seed dirs: %v", err)
	}
	child := []byte("new-dir-child-order-b")
	if err := os.WriteFile(filepath.Join(viewDir, "a", "new", "f.txt"), child, 0o644); err != nil {
		t.Fatalf("seed new-dir child: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "a/new", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed a/new baseline: %v", err)
	}
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fa", Path: "a"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpMkdir, Direction: store.DirOutbound, ViewID: 7, Path: "a/new",
	}); err != nil {
		t.Fatalf("Enqueue mkdir: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to converge in one drain)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (mkdir wedged on errParentUnknown before the fix)", n)
	}
	copyName := soleConflictCopy(t, viewDir)
	got, err := os.ReadFile(filepath.Join(viewDir, copyName, "new", "f.txt"))
	if err != nil || string(got) != string(child) {
		t.Fatalf("preserved new-dir child = %q (err %v), want %q", got, err, child)
	}
}

// TestOutboundUploadLiveParentStillTransient guards the conservative side: a normal
// errParentUnknown wait (the parent's own op has not run yet, and the source is still on disk)
// must stay transient and queued, not be misread as a deleted parent. The source-gone probe
// reports the file present, so the op keeps retrying.
func TestOutboundUploadLiveParentStillTransient(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.MkdirAll(filepath.Join(viewDir, "dir"), 0o755); err != nil {
		t.Fatalf("seed dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "dir", "f.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// Parent baseline present but id-less (its mkdir op has not run yet).
	if err := st.PutBaseline(7, "dir", store.Baseline{FileType: "dir", State: store.StatePending}); err != nil {
		t.Fatalf("seed parent baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "dir/f.txt", LocalSize: 1,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue: want transient error for the unresolved live parent, got nil")
	}
	if len(c.uploads) != 0 {
		t.Errorf("upload attempted despite unknown parent: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("op not left queued: %d", n)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("op dead-lettered on a transient live-parent wait: %d", dl)
	}
}

// TestOutboundRenameSourceNodeDeletedResolves pins the source-node exit: a local
// move a/x -> b/y produced a queued OpRename (baseline relocated to b/y StatePending) while
// another client deleted a/x server-side, BOTH parents staying alive. pc.Rename fails
// ErrPathError (the source is gone), the postcondition QueryNode(b/y) also errors (the move
// never committed), and both parent probes read a/ and b/ present - before the fix nothing else
// in applyOutbound's matrix matched, so the op retried forever while the authoritative inbound
// OpRemove(a/x) skipped behind it on the shared source key every drain: a permanent mutual
// wedge. With the source-node absence proof the rename resolves as a race in one drain (no
// durable trace), the inbound remove applies, and the still-on-disk file at b/y - untracked by
// the resolution - is re-uploaded by the next scan as a fresh create.
func TestOutboundRenameSourceNodeDeletedResolves(t *testing.T) {
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr: actions.ErrPathError,
		// b/y: the move never committed. a/x: the source node was deleted, so the probe that
		// corroborates the source parent's listing answers with the path-error sentinel too.
		queryNodeErrByPath: map[string]error{
			"b/y": actions.ErrPathError,
			"a/x": actions.ErrPathError,
		},
		// Both parents are alive (the root listing shows a/ and b/), and the source parent's
		// clean listing shows the node under NEITHER its id nor its leaf name: positive proof
		// the source NODE itself is gone server-side.
		listResult: map[string][]actions.Node{
			"":  {{Name: "a", FileID: "fa"}, {Name: "b", FileID: "fb"}},
			"a": {{Name: "keep.txt", FileID: "fk"}},
		},
		uploadNode: map[string]actions.Node{"y": {FileID: "fnew", Hash: "h-y", MTime: 9}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	for _, d := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(viewDir, d), 0o755); err != nil {
			t.Fatalf("seed dir %s: %v", d, err)
		}
	}
	// The local move already happened on disk: b/y holds the file, a/x is gone.
	if err := os.WriteFile(filepath.Join(viewDir, "b", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b", store.Baseline{FileID: "fb", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed b baseline: %v", err)
	}
	// localReconcile already relocated the baseline to the destination (StatePending).
	if err := st.PutBaseline(7, "b/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed b/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "b/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// The server delete of a/x reconciles into a serialized inbound OpRemove(a/x) BEHIND the
	// outbound rename (shared source key).
	events := []actions.Event{{SyncID: 5, Action: "delete", FileID: "fx", Path: "a/x"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to resolve within the corroboration passes; mutual wedge before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (mutual wedge before the fix)", n)
	}
	// A race resolution, not a drop: no durable dead-letter trace for either op.
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("race resolution wrote a durable dead-letter trace: %d, want 0", dl)
	}
	// The moved file survives on disk, untracked: the baseline was DELETED (a reverted synced
	// record would have made the trailing inbound remove read the file as server authority
	// and delete it).
	got, err := os.ReadFile(filepath.Join(viewDir, "b", "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("moved file at b/y = %q (err %v), want %q", got, err, moved)
	}
	if b, gerr := st.GetBaseline(7, "b/y"); !isNotFound(gerr) {
		t.Fatalf("b/y baseline not dropped: %+v (err %v)", b, gerr)
	}

	// The next scan re-detects the untracked file and uploads it fresh into the live b/.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue (re-emitted create): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push (nothing re-emitted before the fix)", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fb" || u.name != "y" || string(u.content) != string(moved) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fb/y with the moved content", u.parentID, u.name, u.content)
	}
	if b, err := st.GetBaseline(7, "b/y"); err != nil || b.State != store.StateSynced || b.FileID != "fnew" {
		t.Errorf("final b/y baseline = %+v (err %v), want synced with the assigned id", b, err)
	}
	if conflicts, _ := st.ListConflicts(7); len(conflicts) != 0 {
		t.Errorf("spurious conflicts recorded: %+v", conflicts)
	}
}

// TestOutboundRenameSourceNodeMovedResolves is the server-side-move variant of the source-node
// exit: the other client MOVED a/x to c/z instead of deleting it. From the source parent's listing
// the node reads gone all the same (the walk cannot see a cross-directory move - accepted as
// data-safe), so the stale outbound rename resolves the same way; the inbound OpRename
// (a/x -> c/z) behind it then drains against an untracked source and drops as a cold-start
// gap (one durable trace), and the on-disk file at b/y is re-uploaded fresh - functional
// convergence, with the server-side duplicate of the move-vs-move race tolerated.
func TestOutboundRenameSourceNodeMovedResolves(t *testing.T) {
	moved := []byte("moved-file-content")

	c := &pushClient{
		renameErr: actions.ErrPathError,
		// b/y: the move never committed. a/x: the source node moved away server-side, so
		// nothing occupies its old path and the corroborating probe reports it gone.
		queryNodeErrByPath: map[string]error{
			"b/y": actions.ErrPathError,
			"a/x": actions.ErrPathError,
		},
		listResult: map[string][]actions.Node{
			"":  {{Name: "a", FileID: "fa"}, {Name: "b", FileID: "fb"}, {Name: "c", FileID: "fc"}},
			"a": {{Name: "keep.txt", FileID: "fk"}},
		},
		uploadNode: map[string]actions.Node{"y": {FileID: "fnew", Hash: "h-y", MTime: 9}},
		// The gap-dropped inbound rename leaves an empty c/ behind (fsutil.Rename creates the
		// destination parent before the no-op'ed move), which the next scan pushes as a mkdir.
		mkdirNode: map[string]actions.Node{"c": {FileID: "fdirc", MTime: 7}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	for _, d := range []string{"a", "b"} {
		if err := os.MkdirAll(filepath.Join(viewDir, d), 0o755); err != nil {
			t.Fatalf("seed dir %s: %v", d, err)
		}
	}
	if err := os.WriteFile(filepath.Join(viewDir, "b", "y"), moved, 0o644); err != nil {
		t.Fatalf("seed moved file: %v", err)
	}
	if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed a baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b", store.Baseline{FileID: "fb", FileType: "dir", State: store.StateSynced}); err != nil {
		t.Fatalf("seed b baseline: %v", err)
	}
	if err := st.PutBaseline(7, "b/y", store.Baseline{
		FileID: "fx", FileType: "file", LocalHash: md4.ContentHash(moved),
		LocalSize: int64(len(moved)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed b/y baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
		Path: "a/x", NewPath: "b/y", FileID: "fx",
		LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
	}); err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}
	// The server move a/x -> c/z reconciles into a serialized inbound OpRename behind the
	// outbound rename (relocatePending no-ops: the node's baseline sits at b/y, not a/x).
	events := []actions.Event{{SyncID: 5, Action: "move", FileID: "fx", FromPath: "a/x", Path: "c/z"}}
	if _, _, err := e.reconcile(7, events, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue: %v (want both ops to resolve within the corroboration passes; mutual wedge before the fix)", err)
	}

	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("ops still queued: %d, want 0 (mutual wedge before the fix)", n)
	}
	// The stale outbound rename resolved without a trace; the inbound rename found neither a
	// tracked node nor an on-disk source (the resolution untracked it) and dropped as a
	// cold-start gap with exactly one durable trace.
	if dl, _ := st.DeadLetterLen(); dl != 1 {
		t.Errorf("dead-letter traces = %d, want 1 for the gap-dropped inbound rename", dl)
	}
	// The dropped inbound rename re-requests its node at the destination (finding H2), so the
	// server's copy of fx materialises at c/z instead of the replica staying permanently short of
	// it - the residual this scenario used to accept ("a later modify event or the cold start
	// fetches it") and the two-client stand proved never resolves. The fixture scripts no bytes
	// for fx, so the file is empty; what matters is that it is the SERVER's node, fetched fresh,
	// and not the surviving local file at b/y.
	if _, serr := os.Stat(filepath.Join(viewDir, "c", "z")); serr != nil {
		t.Errorf("c/z not materialised by the dropped rename's compensation: %v", serr)
	}
	got, err := os.ReadFile(filepath.Join(viewDir, "b", "y"))
	if err != nil || string(got) != string(moved) {
		t.Fatalf("moved file at b/y = %q (err %v), want %q", got, err, moved)
	}
	if b, gerr := st.GetBaseline(7, "b/y"); !isNotFound(gerr) {
		t.Fatalf("b/y baseline not dropped: %+v (err %v)", b, gerr)
	}

	// Functional convergence: the next scan re-uploads the surviving file fresh under b/.
	if _, err := e.localReconcile(context.Background(), 7); err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if err := drainSettled(t, e, st); err != nil {
		t.Fatalf("drainQueue (re-emitted create): %v", err)
	}
	if len(c.uploads) != 1 {
		t.Fatalf("uploads = %d, want exactly 1 re-emitted push", len(c.uploads))
	}
	u := c.uploads[0]
	if u.parentID != "fb" || u.name != "y" || string(u.content) != string(moved) {
		t.Errorf("re-emitted upload = parent %q name %q content %q, want fb/y with the moved content", u.parentID, u.name, u.content)
	}
}

// TestOutboundRenameInconclusiveListingStaysQueued guards the conservative side of the source-node
// exit: only POSITIVE proof that the source node has left op.Path may drop the op. A
// probe-timeout listing and an id-less op - which must never be dropped on a name-only listing
// - both leave the rename queued for the next drain, with its relocated pending baseline
// intact.
//
// The third historical case here - a node still visible by id under a DIFFERENT leaf name -
// moved out: it is now a POSITIVE exit (the node was renamed in
// place, so op.Path can never resolve again), pinned by
// TestOutboundRenameSourceRenamedInPlaceResolves.
func TestOutboundRenameInconclusiveListingStaysQueued(t *testing.T) {
	moved := []byte("moved-file-content")

	seed := func(t *testing.T, c *pushClient, fileID string) (*Engine, *store.Store) {
		t.Helper()
		e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
		for _, d := range []string{"a", "b"} {
			if err := os.MkdirAll(filepath.Join(viewDir, d), 0o755); err != nil {
				t.Fatalf("seed dir %s: %v", d, err)
			}
		}
		if err := os.WriteFile(filepath.Join(viewDir, "b", "y"), moved, 0o644); err != nil {
			t.Fatalf("seed moved file: %v", err)
		}
		if err := st.PutBaseline(7, "a", store.Baseline{FileID: "fa", FileType: "dir", State: store.StateSynced}); err != nil {
			t.Fatalf("seed a baseline: %v", err)
		}
		if err := st.PutBaseline(7, "b", store.Baseline{FileID: "fb", FileType: "dir", State: store.StateSynced}); err != nil {
			t.Fatalf("seed b baseline: %v", err)
		}
		if err := st.PutBaseline(7, "b/y", store.Baseline{
			FileID: fileID, FileType: "file", LocalHash: md4.ContentHash(moved),
			LocalSize: int64(len(moved)), State: store.StatePending,
		}); err != nil {
			t.Fatalf("seed b/y baseline: %v", err)
		}
		if _, err := st.Enqueue(store.Op{
			Type: store.OpRename, Direction: store.DirOutbound, ViewID: 7,
			Path: "a/x", NewPath: "b/y", FileID: fileID,
			LocalHash: md4.ContentHash(moved), LocalSize: int64(len(moved)),
		}); err != nil {
			t.Fatalf("Enqueue rename: %v", err)
		}
		return e, st
	}

	liveParents := []actions.Node{{Name: "a", FileID: "fa"}, {Name: "b", FileID: "fb"}}
	cases := []struct {
		name   string
		fileID string
		client func() *pushClient
	}{
		{
			// The source parent's listing itself fails with a non-path error (a probe
			// timeout): absence is inconclusive, never positive.
			name:   "listing timeout is inconclusive",
			fileID: "fx",
			client: func() *pushClient {
				return &pushClient{
					renameErr:          actions.ErrPathError,
					queryNodeErrByPath: map[string]error{"b/y": actions.ErrPathError},
					listResult:         map[string][]actions.Node{"": liveParents},
					listErrByPath:      map[string]error{"a": errors.New("engine: probe timeout")},
				}
			},
		},
		{
			// An id-less rename (planRename never emits one; defensive) must never be
			// dropped on a name-only listing: a clean listing omitting the leaf would
			// falsely read a renamed-away node as gone.
			name:   "id-less rename never dropped on a name-only listing",
			fileID: "",
			client: func() *pushClient {
				return &pushClient{
					renameErr:          actions.ErrPathError,
					queryNodeErrByPath: map[string]error{"b/y": actions.ErrPathError},
					listResult: map[string][]actions.Node{
						"":  liveParents,
						"a": {{Name: "keep.txt", FileID: "fk"}},
					},
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := tc.client()
			e, st := seed(t, c, tc.fileID)

			if err := e.drainQueue(context.Background()); err == nil {
				t.Fatal("drainQueue: want transient error keeping the op queued, got nil")
			}
			if n, _ := st.PendingLen(); n != 1 {
				t.Fatalf("queued ops = %d, want 1 (op must stay queued)", n)
			}
			if dl, _ := st.DeadLetterLen(); dl != 0 {
				t.Errorf("op dead-lettered on an inconclusive probe: %d", dl)
			}
			b, gerr := st.GetBaseline(7, "b/y")
			if gerr != nil || b.State != store.StatePending || b.FileID != tc.fileID {
				t.Fatalf("b/y baseline = %+v (err %v), want intact StatePending with file id %q", b, gerr, tc.fileID)
			}
		})
	}
}
