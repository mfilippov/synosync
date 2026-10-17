package engine

// The reconcile durability pair: an op and the baseline that arms its never-lose gate.
//
// This file also holds the two-engine outcome helpers, since the shapes worth asserting are
// "what content still exists in this replica", not "what is at path p" - the doctrine leaves
// the choice of remedy (kept in place, or set aside as a _Conflict copy) to the apply.

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// viewHoldsContent reports whether any regular file under the view carries exactly content.
func viewHoldsContent(t *testing.T, n *dsmEngine, viewName, content string) bool {
	t.Helper()
	found := false
	root := n.viewDir(viewName)
	err := filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err
		}
		b, rerr := os.ReadFile(p) // #nosec G304 -- a test-owned temp tree
		if rerr != nil {
			return rerr
		}
		if string(b) == content {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return found
}

// viewTree lists every file under the view, relative and slash-separated, for failure output.
func viewTree(t *testing.T, n *dsmEngine, viewName string) []string {
	t.Helper()
	root := n.viewDir(viewName)
	var out []string
	_ = filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() {
			return err //nolint:nilerr // best-effort listing for a failure message
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr == nil {
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out
}

// TestReconcileCreateCommitsOpAndItsGateAtomically pins the create arm's durability.
//
// reconcile used to enqueue an op and mark its baseline pending in TWO bbolt transactions,
// and called the window between them benign because the batch replays. It is not benign for a
// fresh CREATE. The pending baseline is the ONLY thing arming that download's never-lose gate:
// with no record and no byid entry at the target path, untrackedFilePresent reads the occupant
// as "not reconcile-produced" and applyDownload promotes over it with a REPLACING rename and
// no divergence base. And the replay never gets to run - the restart's cycle drains the queue
// BEFORE it re-pulls the batch (coldStartPending → localScan → drainQueue → pollOnce), and
// localScan skips any path that already carries a conflicting queued op.
//
// So the test crashes the write pair itself, after both halves are staged, and demands that
// NEITHER survived: a queue record must never outlive the gate that guards it. It then lets
// the batch replay (the cursor was never advanced) and checks the outcome the doctrine
// promises - the untracked user file is kept, beside the server's content.
func TestReconcileCreateCommitsOpAndItsGateAtomically(t *testing.T) {
	ctx := context.Background()
	d := newFakeDSM()
	d.addView(7, 70, "Photos")
	other := &dsmClient{dsm: d, session: "sess-other"}

	n := newDSMEngine(t, d, "sess-a", false)
	// The user's own file already occupies the path the server is about to create a node at.
	n.write(t, "Photos", "note.txt", "user bytes")

	if _, err := other.Upload(ctx, d.rootFileID(7), "note.txt", 5000, 0, []byte("server bytes")); err != nil {
		t.Fatalf("other client Upload: %v", err)
	}
	evs, _, err := d.pullEvent(7, 0)
	if err != nil {
		t.Fatalf("pullEvent: %v", err)
	}
	if len(evs) != 1 || evs[0].Action != "create" {
		t.Fatalf("events = %+v, want one create", evs)
	}

	// THE CRASH: both writes are staged, then the transaction dies before commit.
	boom := errors.New("crash after staging both writes")
	n.e.batchStore = func(fn func(*store.Batch) error) error {
		return n.st.Batch(func(b *store.Batch) error {
			if ferr := fn(b); ferr != nil {
				return ferr
			}
			return boom
		})
	}
	if _, _, rerr := n.e.reconcile(7, evs, 0); !errors.Is(rerr, boom) {
		t.Fatalf("reconcile err = %v, want the injected crash", rerr)
	}
	if pending, perr := n.st.PendingLen(); perr != nil || pending != 0 {
		t.Fatalf("queue holds %d ops (err %v) after the crash, want 0: the op outlived the gate that guards it",
			pending, perr)
	}
	if _, gerr := n.st.GetBaseline(7, "note.txt"); !isNotFound(gerr) {
		t.Fatalf("GetBaseline err = %v, want not-found: the gate outlived the rolled-back op", gerr)
	}

	// Replay: the cursor was never advanced, so the same batch arrives again.
	n.e.batchStore = n.st.Batch
	if _, _, rerr := n.e.reconcile(7, evs, 0); rerr != nil {
		t.Fatalf("replay reconcile: %v", rerr)
	}
	if err := n.e.drainQueue(ctx); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if !viewHoldsContent(t, n, "Photos", "user bytes") {
		t.Fatalf("the user's untracked file was clobbered by the download; view now holds %v",
			viewTree(t, n, "Photos"))
	}
	if !viewHoldsContent(t, n, "Photos", "server bytes") {
		t.Fatalf("the server's content never landed; view now holds %v", viewTree(t, n, "Photos"))
	}
}
