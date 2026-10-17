//go:build !windows

package engine

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundUploadSourceNonRegularRefused models a regular file replaced by a FIFO before
// the push. noSymlinkEscape passes (a FIFO is not a symlink), so openUploadSource is what
// must refuse it: opened non-blocking, the fstat shows a non-regular node and the op is
// dropped as errUploadSourceGone - without the non-blocking open + regular-file check a
// plain os.ReadFile/os.Open of the FIFO would block the worker forever.
func TestOutboundUploadSourceNonRegularRefused(t *testing.T) {
	c := &pushClient{}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	fifo := filepath.Join(viewDir, "a.txt")
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{FileType: "file", LocalHash: "h", State: store.StatePending}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", LocalHash: "h",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploaded a non-regular source: %+v", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("non-regular-source op not dropped: %d remain", n)
	}
	// Fresh create (no FileID): the pending baseline is dropped so the next scan reconciles.
	if _, err := st.GetBaseline(7, "a.txt"); err == nil {
		t.Error("baseline should be dropped when the source is non-regular")
	}
	_ = os.Remove(fifo)
}
