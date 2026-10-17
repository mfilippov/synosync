//go:build integration

package integration

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// s2lTimeout bounds how long a single server→local propagation may take before the
// scenario fails. It is generous so a slow stand or a long-poll cycle does not flake
// (the engine long-polls the view, pulls its events and downloads over the pool).
const s2lTimeout = 90 * time.Second

// TestIntegration_ServerToLocal drives the inbound happy path end-to-end against a
// live DSM: with the real engine running on a temp sync root it mutates the server
// scratch tree (create, modify, mkdir, rename, remove) via the action helpers, and
// after each change asserts the temp sync root on disk reflects it (content/MD4,
// path, absence) and the local baseline settles to StateSynced. It self-skips
// without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_ServerToLocal(t *testing.T) {
	sc := setupScenario(t)

	// create - a brand-new server file must appear on disk with matching bytes.
	content := []byte("hello synosync: server -> local create\n")
	sc.remotePut(t, "file.txt", content)
	sc.waitLocalContent(t, "file.txt", content)
	assertLocalContent(t, sc, "file.txt", content)
	assertBaselineSynced(t, sc, "file.txt")

	// modify - a server content change must overwrite the local file in place.
	modified := []byte("hello synosync: server -> local MODIFY, longer body now\n")
	sc.remotePut(t, "file.txt", modified)
	sc.waitLocalContent(t, "file.txt", modified)
	assertLocalContent(t, sc, "file.txt", modified)
	assertBaselineSynced(t, sc, "file.txt")

	// mkdir - a new server directory must materialise locally.
	sc.remoteMkdirAll(t, "sub")
	waitForSync(t, sc.eng, s2lTimeout, func() bool {
		return sc.localIsDir(t, "sub")
	})
	if !sc.localIsDir(t, "sub") {
		t.Fatalf("local disk did not reflect server mkdir of %q", "sub")
	}
	assertBaselineSynced(t, sc, "sub")

	// rename - moving the server file under the new directory must move it on disk.
	serverRename(t, sc.client, sc.view.ViewID, sc.view.RootNodeID, sc.viewRel("file.txt"), sc.viewRel("sub/renamed.txt"))
	waitForSync(t, sc.eng, s2lTimeout, func() bool {
		got, ok := sc.readLocal(t, "sub/renamed.txt")
		return !sc.localExists(t, "file.txt") && ok && bytes.Equal(got, modified)
	})
	if sc.localExists(t, "file.txt") {
		t.Fatalf("local disk still has the pre-rename path %q", "file.txt")
	}
	assertLocalContent(t, sc, "sub/renamed.txt", modified)
	assertBaselineSynced(t, sc, "sub/renamed.txt")

	// remove - deleting the renamed server file must delete it on disk.
	serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel("sub/renamed.txt"))
	waitForSync(t, sc.eng, s2lTimeout, func() bool {
		return !sc.localExists(t, "sub/renamed.txt")
	})
	if sc.localExists(t, "sub/renamed.txt") {
		t.Fatalf("local disk still has the removed path %q", "sub/renamed.txt")
	}
	// The surviving directory's baseline stays synced.
	assertBaselineSynced(t, sc, "sub")
}

// waitLocalContent polls until the on-disk file at a scratch-relative path holds
// exactly want, nudging the engine each round.
func (sc *scenario) waitLocalContent(t *testing.T, rel string, want []byte) {
	t.Helper()
	waitForSync(t, sc.eng, s2lTimeout, func() bool {
		got, ok := sc.readLocal(t, rel)
		return ok && bytes.Equal(got, want)
	})
}

// assertLocalContent fails unless the on-disk file at a scratch-relative path exists
// and its bytes equal want.
func assertLocalContent(t *testing.T, sc *scenario, rel string, want []byte) {
	t.Helper()
	got, ok := sc.readLocal(t, rel)
	if !ok {
		t.Fatalf("local disk is missing %q", rel)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("local content for %q mismatch: got %d bytes, want %d bytes", rel, len(got), len(want))
	}
}

// pathAbsent reports whether an os error means "nothing is at that path".
//
// A missing entry is the obvious case; ENOTDIR is the other one, and leaving it out made a
// whole class of scenario untestable. When a peer replaces a FILE with a DIRECTORY of the same
// name, the client that has not applied it yet still holds the file, so a poll for a path
// inside it answers ENOTDIR - "a component is not a directory". That is a legitimate
// not-yet-applied state, and semantically it is the same answer: nothing exists there. Reading
// it as a harness failure aborted the poll on its FIRST iteration, before the engine could have
// applied anything, so the test reported a convergence failure that was really an impatient
// assertion.
//
// What the widening costs, stated exactly, because all three helpers use it and the FINAL
// assertions go through them too (assertLocalContent via readLocal, and localExists/localIsDir
// directly):
//
//   - A POSITIVE assertion keeps its teeth. ENOTDIR makes readLocal report absent, so
//     assertLocalContent still fails - only its message reads "missing" for a path whose
//     ancestor is a file.
//   - A NEGATIVE existence check on a NESTED path can be satisfied by a degenerate ancestor:
//     !localExists("sub/x") is true both when sub/x was removed and when `sub` became a file.
//     Two such sites exist today (a "the deleted thing did not come back" check and a
//     post-rename absence check), and both are preceded by an assertion that already proved the
//     ancestor to be a directory, so neither can be masked as written. A new nested negative
//     check without that predecessor would need its own tighter helper.
func pathAbsent(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, syscall.ENOTDIR)
}

// readLocal reads the on-disk file at a scratch-relative path, reporting whether it
// exists (an absent path is reported absent, any other error fails the test).
func (sc *scenario) readLocal(t *testing.T, rel string) ([]byte, bool) {
	t.Helper()
	b, err := os.ReadFile(sc.localAbs(t, rel))
	if err != nil {
		if pathAbsent(err) {
			return nil, false
		}
		t.Fatalf("integration: read local %q: %v", rel, err)
	}
	return b, true
}

// localExists reports whether anything exists at a scratch-relative on-disk path
// (without following a final symlink).
func (sc *scenario) localExists(t *testing.T, rel string) bool {
	t.Helper()
	_, err := os.Lstat(sc.localAbs(t, rel))
	if err == nil {
		return true
	}
	if pathAbsent(err) {
		return false
	}
	t.Fatalf("integration: lstat local %q: %v", rel, err)
	return false
}

// localIsDir reports whether a scratch-relative on-disk path exists and is a
// directory.
func (sc *scenario) localIsDir(t *testing.T, rel string) bool {
	t.Helper()
	fi, err := os.Stat(sc.localAbs(t, rel))
	if err != nil {
		if pathAbsent(err) {
			return false
		}
		t.Fatalf("integration: stat local %q: %v", rel, err)
	}
	return fi.IsDir()
}

// remotePut uploads content as a file at a scratch-relative path on the server,
// creating the intervening scratch directories first, and returns the created node.
// Re-uploading the same path overwrites it in place (a server-side modify).
func (sc *scenario) remotePut(t *testing.T, rel string, content []byte) actions.Node {
	t.Helper()
	dir, name := splitRel(rel)
	parentID := sc.remoteMkdirAll(t, dir).FileID
	return serverPut(t, sc.client, sc.view.RootNodeID, parentID, name, content)
}

// remoteMkdirAll ensures every directory along the scratch-base-relative path rel
// exists on the server, creating the missing segments from the view root down, and
// returns the deepest node (the scratch base itself when rel is empty). Existing
// segments are reused (idempotent), so repeated calls converge.
func (sc *scenario) remoteMkdirAll(t *testing.T, rel string) actions.Node {
	t.Helper()
	full := sc.viewRel(rel) // e.g. "synosync-it/TestName/sub", no leading slash
	parentID := sc.viewRootID(t)
	var node actions.Node
	var cur string
	for _, seg := range strings.Split(full, "/") {
		if seg == "" {
			continue
		}
		if cur == "" {
			cur = seg
		} else {
			cur += "/" + seg
		}
		if n, ok := serverQuery(t, sc.client, sc.view.ViewID, cur); ok {
			node, parentID = n, n.FileID
			continue
		}
		node = serverMkdir(t, sc.client, sc.view.RootNodeID, parentID, seg)
		parentID = node.FileID
	}
	return node
}

// splitRel splits a slash-separated scratch-relative path into its parent directory
// (empty for a top-level entry) and final leaf name.
func splitRel(rel string) (dir, name string) {
	rel = strings.Trim(rel, "/")
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		return rel[:i], rel[i+1:]
	}
	return "", rel
}
