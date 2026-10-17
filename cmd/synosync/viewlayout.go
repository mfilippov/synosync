package main

// The one gate every writer of config.Views goes through.
//
// There are two of them - `pair` and `config set views.selected` - and the first
// version of this feature guarded only the second. That gap was enough: a
// re-pair of an established single-view install silently flipped the layout,
// the engine then resolved every path to a location holding nothing, read that
// as a local deletion of the whole tree, and pushed those deletions to the
// server. One gate, called by both, is the point.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
)

// errSyncRootNotEmpty reports that a flat layout was asked for over a directory
// that already holds something.
var errSyncRootNotEmpty = errors.New("sync root is not empty")

// errLayoutLocked reports a refused change to what occupies the sync root.
var errLayoutLocked = errors.New("the sync root's layout cannot change once files are synced")

// guardViewChange decides whether a view selection may be written.
//
// It compares LAYOUT IDENTITY - what occupies the sync root - not the selection.
// Deselecting a view while the layout holds is always fine: it stops syncing and
// leaves the local files alone, the same treatment a view the server stopped
// enumerating already gets. Changing the identity is what relocates a tree.
//
// Two rules, in order:
//
//  1. The identity may not change once anything is synced - or merely queued.
//     Baselines survive a
//     relocation - they are keyed by (view_id, view-relative path) and never
//     encode the directory - but the engine would look in the new place, find
//     nothing, and read that as a local deletion of everything.
//
//  2. Turning the flat layout on requires an empty sync root. With a
//     subdirectory per view the sync root is only a container and a file beside
//     those directories is outside every view; flattened, the sync root IS the
//     view, so anything already in it is in scope and the first cycle uploads it.
//     Reached only when rule 1 passed, i.e. when nothing is synced yet.
func guardViewChange(st *store.Store, old, next config.Views, syncRoot string, stderr io.Writer) error {
	if old.LayoutIdentity() == next.LayoutIdentity() {
		return nil
	}

	synced, err := storeHasWork(st)
	if err != nil {
		// Unknown: refuse rather than relocate a tree that might exist. The
		// operator can retry; the alternative failure is unrecoverable.
		fmt.Fprintf(stderr, "synosync: cannot tell whether anything is synced yet: %s\n", userErr(err))
		return err
	}
	if synced {
		writeLayoutRefusal(stderr, old, next)
		return errLayoutLocked
	}
	if next.Flat {
		return requireEmptySyncRoot(syncRoot)
	}
	return nil
}

// storeHasWork reports whether anything on disk is tied to the current layout.
//
// Baselines are the usual evidence, but not the only one: the enqueue-first
// doctrine writes a queue record BEFORE its pending baseline, so a crash in that
// window leaves durable ops whose serialisation keys the engine would recompute
// under a new layout. Either is enough to refuse.
func storeHasWork(st *store.Store) (bool, error) {
	synced, err := st.HasBaselines()
	if err != nil || synced {
		return synced, err
	}
	return st.HasQueuedOps()
}

// writeLayoutRefusal explains the refusal in terms of where files are.
//
// It names `unlink` and nothing else on purpose. An earlier version advised
// moving the tree and setting the value again - which could never pass, because
// the gate keys on whether the store holds work and moving files changes neither
// a baseline nor a queue record.
// Worse, following it left the files in the NEW layout while the config still
// described the OLD one, which is exactly the state the gate exists to prevent.
func writeLayoutRefusal(stderr io.Writer, old, next config.Views) {
	fmt.Fprintf(stderr, "synosync: this would move every synced file.\n\n")
	fmt.Fprintf(stderr, "  now:  %s\n", describeIdentity(old))
	fmt.Fprintf(stderr, "  new:  %s\n\n", describeIdentity(next))
	fmt.Fprintf(stderr, "  The layout is fixed once files are synced: the engine would look for the\n")
	fmt.Fprintf(stderr, "  whole tree in its new location, find nothing, and report every file as\n")
	fmt.Fprintf(stderr, "  deleted - to the server.\n\n")
	fmt.Fprintf(stderr, "  To change it:\n")
	fmt.Fprintf(stderr, "    1. stop the daemon\n")
	fmt.Fprintf(stderr, "    2. %s\n", relocationStep(old, next))
	fmt.Fprintf(stderr, "    3. synosync unlink, then pair again with the new selection\n")
}

// relocationStep names the move the operator has to make themselves.
//
// "Run unlink and pair again" alone was a dead end in one direction: unlink
// resets the store but leaves the files, so re-pairing INTO a flat layout then
// met the empty-sync-root check over the very tree that was just synced, and
// refused a second time. The move has to happen, and only the operator can
// decide where their files go.
func relocationStep(old, next config.Views) string {
	switch {
	case next.Flat:
		return "move the selected view's subdirectory contents up into the sync root, " +
			"and move the other views' directories out of it"
	case old.Flat:
		return "move the sync root's contents down into a subdirectory named after the view"
	default:
		return "move each view's files into the layout you are switching to"
	}
}

// describeIdentity renders a layout identity for a human.
func describeIdentity(v config.Views) string {
	if v.Flat && len(v.Selected) == 1 {
		name := v.Selected[0].Name
		if name == "" {
			name = fmt.Sprintf("view %d", v.Selected[0].ID)
		}
		return fmt.Sprintf("%s directly in the sync root", name)
	}
	return "one subdirectory per view"
}

// requireEmptySyncRoot refuses a flat layout over a directory that already holds
// something. A path that does not exist yet is as empty as it gets - the daemon
// creates it.
func requireEmptySyncRoot(syncRoot string) error {
	entries, err := os.ReadDir(syncRoot)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("read the sync root %q: %w", syncRoot, err)
	case len(entries) == 0:
		return nil
	}

	shown := make([]string, 0, 5)
	for _, e := range entries {
		if len(shown) == cap(shown) {
			shown = append(shown, fmt.Sprintf("… and %d more", len(entries)-len(shown)))
			break
		}
		shown = append(shown, e.Name())
	}
	return fmt.Errorf("%w: %s already holds %s.\n"+
		"  Syncing a single view puts its contents directly in the sync root, so\n"+
		"  everything already there would be uploaded to the server on the first\n"+
		"  cycle. Point the sync root at an empty directory, or move these aside first",
		errSyncRootNotEmpty, syncRoot, strings.Join(shown, ", "))
}
