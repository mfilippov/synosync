package engine

import (
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// TestColdStartPlansDescendantsOfAQueuedDirectory pins which queue question the cold-start
// merge asks.
//
// coldStartReconcile used to skip a catalog row whose fold key `conflict`ed with the durable
// queue - an ancestor/descendant OVERLAP, which is the EXECUTOR's serialisation rule ("may
// this path be acted on right now"), not ownership. A cold start that committed some of its
// batches and then failed leaves ops queued; on the retry every descendant of such an op's
// directory was skipped, and the view was nevertheless marked InitialSyncDone at the end.
// Those server-only nodes then became unreachable: the forward poll starts past them and cold
// start never runs again.
//
// `owns` (exact path) is the question the merge actually wants, and duplicate work is not a
// risk - the executor already serialises the overlap on its own, so a child's op simply waits
// behind the directory's.
func TestColdStartPlansDescendantsOfAQueuedDirectory(t *testing.T) {
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	d.seedDir(7, "dir")
	d.seedFile(7, "dir/child.txt", []byte("server child"), 1000)

	n := newDSMEngine(t, d, "sess-a", true)

	// What a previous, partially-failed cold-start attempt left behind: the directory's own
	// op, still queued, still undrained.
	if _, err := n.st.Enqueue(store.Op{
		Type:      store.OpMkdir,
		Direction: store.DirInbound,
		ViewID:    7,
		Path:      "dir",
		FileID:    d.fileIDAt(7, "dir"),
	}); err != nil {
		t.Fatalf("seed the leftover op: %v", err)
	}

	n.settle(t, 3)

	got, ok := n.read(t, "Photos", "dir/child.txt")
	if !ok || got != "server child" {
		t.Fatalf("dir/child.txt = %q present=%v, want the server's content; view holds %v",
			got, ok, strings.Join(viewTree(t, n, "Photos"), ", "))
	}
}
