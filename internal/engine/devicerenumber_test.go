package engine

// Exits from the view-root device guard.
//
// The empty-scan branch consults the recorded device id so a dropped/swapped mount is never read
// as a whole-view delete, but it had no way OUT: an empty view can produce no content evidence by
// construction, so a device id that changed under it - the ordinary result of a reboot
// renumbering a dm/LVM/btrfs/network filesystem - suppressed every later scan and reset the
// debounce forever. These tests pin the bounded exit (a sibling view of the same filesystem that
// adopted the very same transition on content evidence), that a genuine mount swap still supplies
// no such corroboration, and the unreadable-id mirror: Warn once per streak and re-check the
// remembered evidence candidate instead of re-running the capped sweep every pass.

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// newDeviceGuardEngine builds an engine over a real temp sync root holding one on-disk directory
// per named view (view ids 7, 8, …), with a capturing logger so the guard's Warns can be asserted.
func newDeviceGuardEngine(t *testing.T, names ...string) (*Engine, *logCapture, string, []uint64) {
	t.Helper()
	st := newTestStore(t)
	root := t.TempDir()
	ids := make([]uint64, 0, len(names))
	for i, name := range names {
		id := uint64(7 + i)
		if err := st.PutView(id, store.View{Name: name, Privilege: "rw"}); err != nil {
			t.Fatalf("PutView %s: %v", name, err)
		}
		if err := os.MkdirAll(filepath.Join(root, name), 0o755); err != nil {
			t.Fatalf("mkdir view dir %s: %v", name, err)
		}
		ids = append(ids, id)
	}
	h := &logCapture{}
	e := New(st, &fakeTransport{client: &fakeClient{}}, root, "sess", slog.New(h))
	return e, h, root, ids
}

// warnHasAttr reports whether some captured Warn whose message contains substr carries a non-empty
// attribute named key - the operator-facing remedy the suppression Warn must name.
func warnHasAttr(h *logCapture, substr, key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level != slog.LevelWarn || !strings.Contains(r.Message, substr) {
			continue
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key && a.Value.String() != "" {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

// TestEmptyScanAdoptsCorroboratedDeviceRenumber drives the case: the user empties a view, the
// daemon stops before the empty-scan debounce confirms it, and a reboot renumbers the filesystem
// both views live on. The emptied view can offer no content evidence, but its SIBLING walks its
// still-tracked tree and adopts the same old→new transition - corroboration that the filesystem
// was renumbered, not swapped. The emptied view must then adopt the new id and let its ordinary
// debounce converge on the whole-view delete. Before the fix every scan was suppressed and the
// debounce reset, so the delete never propagated and the misleading "mount swap" Warn repeated
// forever.
func TestEmptyScanAdoptsCorroboratedDeviceRenumber(t *testing.T) {
	e, h, root, ids := newDeviceGuardEngine(t, "Photos", "Docs")
	advance := stubClock(e)
	photos, docs := ids[0], ids[1]

	ours := []byte("still ours after the reboot")
	writeFile(t, filepath.Join(root, "Photos"), "keep.txt", ours)
	putSynced(t, e, photos, "keep.txt", "fk", "file", md4.ContentHash(ours), int64(len(ours)))
	if err := os.MkdirAll(filepath.Join(root, "Docs", "sub"), 0o755); err != nil {
		t.Fatalf("mkdir Docs/sub: %v", err)
	}
	putSynced(t, e, docs, "sub", "fsub", "dir", "", 0)

	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }
	for _, id := range ids {
		ops, err := e.localReconcile(context.Background(), id)
		if err != nil {
			t.Fatalf("healthy scan of view %d: %v", id, err)
		}
		if len(ops) != 0 {
			t.Fatalf("healthy scan of view %d produced %d ops, want 0: %+v", id, len(ops), ops)
		}
	}

	// The user empties Docs; a reboot renumbers the filesystem underneath BOTH views.
	if err := os.RemoveAll(filepath.Join(root, "Docs", "sub")); err != nil {
		t.Fatalf("remove Docs/sub: %v", err)
	}
	curDev = 2

	var lastOps []store.Op
	for i := 0; i < emptyScanConfirmThreshold; i++ {
		if _, err := e.localReconcile(context.Background(), photos); err != nil { // adopts on content evidence
			t.Fatalf("post-renumber Photos scan %d: %v", i, err)
		}
		ops, err := e.localReconcile(context.Background(), docs)
		if err != nil {
			t.Fatalf("post-renumber Docs scan %d: %v", i, err)
		}
		lastOps = ops
		advance(emptyScanConfirmMinSpacing) // the debounce counts wall clock, not just scans
	}
	if len(lastOps) != 1 || lastOps[0].Type != store.OpRemove || lastOps[0].Path != "sub" {
		t.Fatalf("corroborated empty-out never converged (ops = %+v, debounce = %d)", lastOps, e.emptyViewScans[docs].count)
	}
	if got := e.viewRootDev[docs]; got != 2 {
		t.Errorf("emptied view device = %d, want the corroborated 2 adopted", got)
	}
	v, err := e.store.GetView(docs)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.RootDevice != 2 {
		t.Errorf("persisted RootDevice = %d, want 2", v.RootDevice)
	}
	if n := h.countWarns("another view of the same filesystem confirmed the same renumbering"); n != 1 {
		t.Errorf("corroboration Warn fired %d times, want exactly 1 (the adopting pass)", n)
	}
}

// TestEmptyScanForeignMountStaysSuppressed is the never-lose side of the same exit: a REAL mount
// swap (a foreign tree on a different device) supplies no corroboration, because the sibling
// view's own scan is suppressed for want of content evidence. The emptied view must therefore stay
// suppressed for as many scans as it takes, with the debounce never advancing - and the Warn must
// say honestly what happened and what the operator can do.
func TestEmptyScanForeignMountStaysSuppressed(t *testing.T) {
	e, h, root, ids := newDeviceGuardEngine(t, "Photos", "Docs")
	photos, docs := ids[0], ids[1]

	ours := []byte("our photos")
	writeFile(t, filepath.Join(root, "Photos"), "keep.txt", ours)
	putSynced(t, e, photos, "keep.txt", "fk", "file", md4.ContentHash(ours), int64(len(ours)))
	if err := os.MkdirAll(filepath.Join(root, "Docs", "sub"), 0o755); err != nil {
		t.Fatalf("mkdir Docs/sub: %v", err)
	}
	putSynced(t, e, docs, "sub", "fsub", "dir", "", 0)

	var curDev uint64 = 1
	e.deviceID = func(string) (uint64, error) { return curDev, nil }
	for _, id := range ids {
		if _, err := e.localReconcile(context.Background(), id); err != nil {
			t.Fatalf("healthy scan of view %d: %v", id, err)
		}
	}

	// The wrong volume is mounted at the sync root: Photos holds a stranger's tree, Docs is an
	// empty directory on it.
	if err := os.RemoveAll(filepath.Join(root, "Photos", "keep.txt")); err != nil {
		t.Fatalf("remove keep.txt: %v", err)
	}
	writeFile(t, filepath.Join(root, "Photos"), "stranger.txt", []byte("not ours"))
	if err := os.RemoveAll(filepath.Join(root, "Docs", "sub")); err != nil {
		t.Fatalf("remove Docs/sub: %v", err)
	}
	curDev = 2

	for i := 0; i < emptyScanConfirmThreshold+3; i++ {
		for _, id := range ids {
			ops, err := e.localReconcile(context.Background(), id)
			if err != nil {
				t.Fatalf("foreign-mount scan %d of view %d: %v", i, id, err)
			}
			if len(ops) != 0 {
				t.Fatalf("foreign-mount scan %d of view %d produced %d ops, want 0: %+v", i, id, len(ops), ops)
			}
		}
	}
	if got := e.emptyViewScans[docs].count; got != 0 {
		t.Errorf("foreign mount advanced the empty debounce to %d, want 0", got)
	}
	if len(e.deviceMoves) != 0 {
		t.Errorf("a suppressed foreign-tree scan recorded corroboration: %+v", e.deviceMoves)
	}
	if got := e.viewRootDev[docs]; got != 1 {
		t.Errorf("emptied view device = %d, want the recorded 1 kept", got)
	}
	if n, _ := e.store.PendingLen(); n != 0 {
		t.Errorf("queue not empty: a mount swap enqueued %d ops", n)
	}
	if got := baselineState(t, e, docs, "sub"); got != store.StateSynced {
		t.Errorf("baseline sub state = %v, want StateSynced (untouched)", got)
	}
	const suppressed = "an empty view can offer no content evidence"
	if h.countWarns(suppressed) == 0 {
		t.Fatal("suppressed empty scan logged no honest Warn")
	}
	for _, key := range []string{"cause", "remedy"} {
		if !warnHasAttr(h, suppressed, key) {
			t.Errorf("suppression Warn carries no %q attribute", key)
		}
	}
}

// putHintedBaseline records a synced file baseline whose cheap-scan hint (size, mtime) matches the
// file on disk while its recorded hash does NOT match the bytes - a decoy that passes the device
// guard's pre-filter and fails the re-hash, i.e. the shape a foreign tree of coincidental
// (size, mtime) matches presents.
func putHintedBaseline(t *testing.T, e *Engine, viewID uint64, viewDir, rel, hash string) {
	t.Helper()
	fi, err := os.Stat(filepath.Join(viewDir, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("stat %s: %v", rel, err)
	}
	b := store.Baseline{
		FileID: "f" + rel, FileType: "file",
		ServerHash: hash, LocalHash: hash,
		LocalSize: fi.Size(), LocalMTime: fi.ModTime().Unix(),
		State: store.StateSynced,
	}
	if err := e.store.PutBaseline(viewID, rel, b); err != nil {
		t.Fatalf("PutBaseline %s: %v", rel, err)
	}
}

// TestUnreadableDeviceStreakIsDampenedAndRecovers is the mirror case: a filesystem that
// stably refuses to report a device id (FUSE, a network redirector) re-enters the changed-device
// arm on EVERY scan pass, because there is no id to record that would end it. It must therefore
// (a) Warn once per streak rather than rolling the log over, and (b) re-check the file that last
// supplied content evidence instead of re-running the maxDeviceConfirmHashes-capped sweep - here
// the tracked tree also holds more capped-out decoy candidates than the sweep may hash, so only
// the remembered candidate can still confirm. When the id reads again it is adopted and the streak
// state is cleared.
func TestUnreadableDeviceStreakIsDampenedAndRecovers(t *testing.T) {
	e, h, root, ids := newDeviceGuardEngine(t, "Photos")
	viewID := ids[0]
	viewDir := filepath.Join(root, "Photos")

	ours := []byte("our content")
	writeFile(t, viewDir, "z.txt", ours)
	putSynced(t, e, viewID, "z.txt", "fz", "file", md4.ContentHash(ours), int64(len(ours)))

	dev := uint64(5)
	readable := true
	e.deviceID = func(string) (uint64, error) {
		if !readable {
			return 0, errors.New("statfs unsupported")
		}
		return dev, nil
	}
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("healthy scan: %v", err)
	}

	// The mount stops reporting an id. The first such pass runs the full sweep and confirms
	// z.txt, remembering it.
	readable = false
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("first unreadable scan: %v", err)
	}

	// Now the view also tracks more decoy candidates than the sweep may hash: they all pass the
	// cheap pre-filter (the scan copies their recorded hash) and all fail the re-hash, so a full
	// sweep exhausts its cap before it ever reaches z.txt.
	for i := 0; i <= maxDeviceConfirmHashes; i++ {
		rel := "d" + string(rune('0'+i)) + ".txt"
		writeFile(t, viewDir, rel, []byte("foreign bytes"))
		putHintedBaseline(t, e, viewID, viewDir, rel, md4.ContentHash(ours))
	}
	// A brand-new local file proves the pass reached the diff instead of being suppressed.
	writeFile(t, viewDir, "new.txt", []byte("fresh"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("second unreadable scan: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload || ops[0].Path != "new.txt" {
		t.Fatalf("unreadable-device pass ops = %+v, want a single OpUpload of new.txt (the remembered candidate must still confirm)", ops)
	}
	if n := h.countWarns("treating root as unavailable"); n != 1 {
		t.Errorf("unreadable-id Warn fired %d times across the streak, want 1", n)
	}
	if n := h.countWarns("proceeding without a device baseline"); n != 1 {
		t.Errorf("proceed Warn fired %d times across the streak, want 1", n)
	}
	if st := e.deviceGuard[viewID]; st.confirmPath != "z.txt" {
		t.Errorf("remembered confirm candidate = %q, want z.txt", st.confirmPath)
	}

	// The id reads again, renumbered: it is adopted (the remembered candidate confirms) and the
	// streak state is cleared.
	readable, dev = true, 6
	if _, err := e.localReconcile(context.Background(), viewID); err != nil {
		t.Fatalf("recovered scan: %v", err)
	}
	if got := e.viewRootDev[viewID]; got != 6 {
		t.Errorf("recovered device = %d, want the renumbered 6 adopted", got)
	}
	if st := e.deviceGuard[viewID]; st.unreadable || st.proceedWarned {
		t.Errorf("unreadable-streak state survived recovery: %+v", st)
	}
	if got, ok := e.deviceMoves[5]; !ok || got != 6 {
		t.Errorf("adopted move 5→%d (recorded=%v), want the transition available for corroboration", got, ok)
	}
	var infos int
	h.mu.Lock()
	for _, r := range h.records {
		if r.Level == slog.LevelInfo && strings.Contains(r.Message, "device id readable again") {
			infos++
		}
	}
	h.mu.Unlock()
	if infos != 1 {
		t.Errorf("recovery logged %d readable-again lines, want 1", infos)
	}
}
