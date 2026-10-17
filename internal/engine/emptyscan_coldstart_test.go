package engine

// These tests cover a data-loss window: coldStartView must record the view
// root's filesystem device id while the mount is known healthy, so the empty-scan mount-swap
// guard (viewRootDeviceChanged) has a baseline to compare against from the very first empty scan
// of a freshly cold-started view. Without the recording the guard is inert for the whole
// first-sync window (RootDevice == 0 → viewRootDeviceChanged returns false), and a mount that
// drops in that window presents as an ordinary empty directory the empty-scan debounce erases the
// whole remote view over.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// coldStartWithSyncedFile drives a full cold start over a scripted catalog whose single node
// matches an on-disk local file, so coldStartReconcile adopts it as a StateSynced baseline and
// the view directory is present on disk when coldStartView completes. It returns the engine, the
// view id and the view directory. The deviceID seam is set to report dev while the cold start
// runs, so the recording captures dev as the healthy baseline.
func coldStartWithSyncedFile(t *testing.T, dev uint64) (*Engine, uint64, string) {
	t.Helper()
	st := newTestStore(t)
	const viewID = 7
	if err := st.PutView(viewID, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "V")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("mkdir view dir: %v", err)
	}
	content := []byte("same content")
	if err := os.WriteFile(filepath.Join(viewDir, "same.txt"), content, 0o644); err != nil {
		t.Fatalf("write same.txt: %v", err)
	}
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("same.txt", "s1", md4.ContentHash(content), int64(len(content))),
	})}
	e := New(st, &catalogTransport{client: c}, root, "sess", nil)
	e.deviceID = func(string) (uint64, error) { return dev, nil }

	if err := e.coldStartView(context.Background(), listViewOf(t, st, viewID)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	// The adopt must have produced a synced baseline (so a later vanished scan would diff as a
	// delete) and the cold start must have recorded the healthy device id.
	b, err := st.GetBaseline(viewID, "same.txt")
	if err != nil {
		t.Fatalf("GetBaseline same.txt: %v", err)
	}
	if b.State != store.StateSynced {
		t.Fatalf("same.txt baseline state = %q, want synced after adopt", b.State)
	}
	return e, viewID, viewDir
}

// coldStartServerOnly drives a full cold start over a catalog whose single node has NO matching
// local file, so coldStartReconcile plans an inbound OpDownload (a StatePending baseline) and the
// view root is NOT materialised on disk during the cold-start scan - the canonical fresh cold
// start the adoption helper above does not exercise. The deviceID seam models production
// osDeviceID: it stats the path and fails (as ENOENT would) when the path is absent, so the
// recording succeeds only if the view root was actually created. It returns the engine, the view
// id and the (post-fix) view directory path.
func coldStartServerOnly(t *testing.T, dev uint64) (*Engine, uint64, string) {
	t.Helper()
	st := newTestStore(t)
	const viewID = 9
	if err := st.PutView(viewID, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir() // the sync root exists; the view subdirectory does NOT until downloads drain
	content := []byte("server content")
	c := &catalogClient{tree: treeFromCatalog([]actions.Event{
		serverFile("only.txt", "s1", md4.ContentHash(content), int64(len(content))),
	})}
	e := planOnly(New(st, &catalogTransport{client: c}, root, "sess", nil))
	e.deviceID = func(p string) (uint64, error) {
		if _, err := os.Stat(p); err != nil {
			return 0, err // model osDeviceID: a missing view root cannot be stat'd
		}
		return dev, nil
	}

	if err := e.coldStartView(context.Background(), listViewOf(t, st, viewID)); err != nil {
		t.Fatalf("coldStartView: %v", err)
	}
	// The server-only node must be a pending inbound download (not adopted): the root is
	// unmaterialised at scan time, which is precisely what makes the recording no-op without the fix.
	b, err := st.GetBaseline(viewID, "only.txt")
	if err != nil {
		t.Fatalf("GetBaseline only.txt: %v", err)
	}
	if b.State != store.StatePending {
		t.Fatalf("only.txt baseline state = %q, want pending (server-only download)", b.State)
	}
	return e, viewID, filepath.Join(root, "V")
}

// TestColdStartRecordsRootDeviceForServerOnlyView is the server-download-born counterpart of
// TestColdStartRecordsRootDeviceForEmptyScanGuard: a fresh cold start with NO
// local files to adopt still records the healthy root device id, because coldStartView
// materialises the view root before recording. Before the fix the root does not exist at
// cold-start-scan time, deviceID (modelling production's stat) fails, RootDevice stays 0, and the
// empty-scan mount-swap guard is inert for the whole first-sync window - a mount that drops after
// the queued downloads land but before the first non-empty scan lets the debounce erase the whole
// remote view.
func TestColdStartRecordsRootDeviceForServerOnlyView(t *testing.T) {
	e, viewID, viewDir := coldStartServerOnly(t, 1)

	// The eager root-create + record must have materialised the view root and persisted the
	// healthy device id to the durable view record (so the guard also survives a restart).
	if _, err := os.Stat(viewDir); err != nil {
		t.Fatalf("view root not materialised during cold start: %v", err)
	}
	v, err := e.store.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 1 {
		t.Fatalf("persisted RootDevice = %d, want 1 (cold start must record the healthy device for a server-only view)", v.RootDevice)
	}

	// Simulate the queued download draining: the baseline becomes StateSynced, the file lands on
	// disk and the cold-start OpDownload is Acked out of the queue - exactly the post-drain state
	// drainQueue would leave, in which the mount-drop window opens.
	b, err := e.store.GetBaseline(viewID, "only.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	b.State = store.StateSynced
	if err := e.store.PutBaseline(viewID, "only.txt", b); err != nil {
		t.Fatalf("PutBaseline synced: %v", err)
	}
	if err := os.WriteFile(filepath.Join(viewDir, "only.txt"), []byte("server content"), 0o644); err != nil {
		t.Fatalf("materialise only.txt: %v", err)
	}
	pend, err := e.store.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	for _, qo := range pend {
		if err := e.store.Ack(qo.Seq); err != nil {
			t.Fatalf("Ack cold-start op seq %d: %v", qo.Seq, err)
		}
	}

	// The mount drops: the file is gone (it lived on the detached mount) but the view root still
	// resolves to a present, empty directory on the parent fs, on a DIFFERENT device.
	if err := os.Remove(filepath.Join(viewDir, "only.txt")); err != nil {
		t.Fatalf("remove only.txt: %v", err)
	}
	e.deviceID = func(string) (uint64, error) { return 2, nil } // swapped underneath

	for i := 0; i < emptyScanConfirmThreshold+3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d on a device-swapped server-only root produced %d ops, want 0 (mount-swap guard): %+v", i, len(ops), ops)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("device-swapped server-only root advanced the debounce to %d, want 0 (never counted)", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a post-cold-start mount swap let the debounce wipe the server-only view: %d", n)
	}
}

// TestColdStartRecordsRootDeviceForEmptyScanGuard is the primary regression: after a cold
// start, a mount swap (the device id changes underneath an otherwise-empty view root) is caught
// by viewRootDeviceChanged and never propagated as a whole-view delete. Before the fix
// (recordViewRootDevice not called from coldStartView) RootDevice stays 0, viewRootDeviceChanged
// returns false, and the empty-scan debounce reaches its threshold and emits an OpRemove for
// every synced baseline - erasing the entire remote view.
func TestColdStartRecordsRootDeviceForEmptyScanGuard(t *testing.T) {
	e, viewID, viewDir := coldStartWithSyncedFile(t, 1)

	// The healthy device id must have been persisted to the durable view record so the guard
	// also survives a restart (see TestColdStartRootDevicePersistsAcrossRestart).
	v, err := e.store.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 1 {
		t.Fatalf("persisted RootDevice = %d, want 1 (cold start must record the healthy device)", v.RootDevice)
	}

	// The mount drops and is replaced by a different underlying filesystem: the on-disk tree is
	// now empty (the file lived on the detached mount) but the view directory still resolves to
	// a present, empty directory on the parent fs - the shape Lstat cannot tell from a genuine
	// empty-out. Only the device id, now different, distinguishes the two.
	if err := os.Remove(filepath.Join(viewDir, "same.txt")); err != nil {
		t.Fatalf("remove same.txt: %v", err)
	}
	e.deviceID = func(string) (uint64, error) { return 2, nil } // swapped underneath

	for i := 0; i < emptyScanConfirmThreshold+3; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("scan %d on a device-swapped root produced %d ops, want 0 (mount-swap guard): %+v", i, len(ops), ops)
		}
	}
	if got := e.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("device-swapped root advanced the debounce to %d, want 0 (never counted)", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a post-cold-start mount swap let the debounce wipe the view: %d", n)
	}
}

// TestColdStartLegitDeleteStillConverges confirms the cold-start device recording does not
// over-suppress: when the device id is unchanged (the mount is the same, the user genuinely
// emptied the view), the empty-scan debounce still advances and propagates the whole-view delete
// after the threshold, exactly as before.
func TestColdStartLegitDeleteStillConverges(t *testing.T) {
	e, viewID, viewDir := coldStartWithSyncedFile(t, 1)
	advance := stubClock(e)

	// The user genuinely deletes the view's contents on the SAME device.
	if err := os.Remove(filepath.Join(viewDir, "same.txt")); err != nil {
		t.Fatalf("remove same.txt: %v", err)
	}
	// device seam unchanged: still reports 1.
	var lastOps []store.Op
	for i := 0; i < emptyScanConfirmThreshold; i++ {
		ops, err := e.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("empty scan %d: %v", i, err)
		}
		lastOps = ops
		advance(emptyScanConfirmMinSpacing) // the debounce counts wall clock, not just scans
	}
	if len(lastOps) == 0 {
		t.Fatalf("legitimate empty-out never converged; debounce stuck at %d", e.emptyViewScans[viewID].count)
	}
	var sawRemove bool
	for _, op := range lastOps {
		if op.Type == store.OpRemove && op.Path == "same.txt" {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Fatalf("threshold reached but no OpRemove(same.txt) emitted: %+v", lastOps)
	}
}

// TestColdStartRootDevicePersistsAcrossRestart confirms the cold-start-recorded device id is
// durable: a fresh engine over the same store (its in-memory viewRootDev map empty, as on a
// restart) still suppresses an empty scan whose device id no longer matches the persisted
// baseline - closing the gap where a restart into an already-dropped/swapped mount, for a view
// whose only healthy observation was its cold start, would erase the remote view.
func TestColdStartRootDevicePersistsAcrossRestart(t *testing.T) {
	e, viewID, viewDir := coldStartWithSyncedFile(t, 42)

	v, err := e.store.GetView(viewID)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 42 {
		t.Fatalf("persisted RootDevice = %d, want 42", v.RootDevice)
	}

	// Restart: a brand-new engine over the same store and root, in-memory map empty. The mount
	// has swapped (the file is gone, the underlying device differs from the persisted baseline).
	if err := os.Remove(filepath.Join(viewDir, "same.txt")); err != nil {
		t.Fatalf("remove same.txt: %v", err)
	}
	e2 := New(e.store, &fakeTransport{client: &fakeClient{}}, e.syncRoot, "sess", nil)
	e2.deviceID = func(string) (uint64, error) { return 99, nil } // device swapped underneath
	for i := 0; i < emptyScanConfirmThreshold+2; i++ {
		ops, err := e2.localReconcile(context.Background(), viewID)
		if err != nil {
			t.Fatalf("post-restart empty scan %d: %v", i, err)
		}
		if len(ops) != 0 {
			t.Fatalf("post-restart scan %d produced %d ops, want 0 (persisted cold-start device guard): %+v", i, len(ops), ops)
		}
	}
	if got := e2.emptyViewScans[viewID].count; got != 0 {
		t.Errorf("post-restart debounce advanced to %d, want 0 (never counted)", got)
	}
	if n, _ := e2.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a restart into a swapped mount let the debounce wipe the view: %d", n)
	}
}
