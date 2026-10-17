//go:build integration

package integration

import (
	"bytes"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// ltsTimeout bounds how long a single local→server propagation may take before the
// scenario fails. It is generous so a slow stand or a busy long-poll cycle does not
// flake (the engine throttles localScan to scanInterval and drains over the pool).
const ltsTimeout = 90 * time.Second

// TestIntegration_LocalToServer drives the outbound happy path end-to-end against a
// live DSM: with the real engine running on a temp sync root it creates, modifies,
// mkdirs, renames and removes on disk, and after each change asserts the server
// reflects it (content/MD4, path, absence) and the local baseline settles to
// StateSynced. It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_LocalToServer(t *testing.T) {
	sc := setupScenario(t)

	// create - a brand-new local file must appear on the server with matching bytes.
	content := []byte("hello synosync: local -> server create\n")
	sc.writeLocal(t, "file.txt", content)
	sc.waitServerHash(t, "file.txt", content)
	assertServerContent(t, sc, "file.txt", content)
	assertBaselineSynced(t, sc, "file.txt")

	// modify - a content change must overwrite the server node in place.
	modified := []byte("hello synosync: local -> server MODIFY, longer body now\n")
	sc.writeLocal(t, "file.txt", modified)
	sc.waitServerHash(t, "file.txt", modified)
	assertServerContent(t, sc, "file.txt", modified)
	assertBaselineSynced(t, sc, "file.txt")

	// mkdir - a new local directory must materialise as a server directory.
	sc.mkdirLocal(t, "sub")
	waitForSync(t, sc.eng, ltsTimeout, func() bool {
		n, ok := sc.queryRel(t, "sub")
		return ok && n.IsDir
	})
	if n, ok := sc.queryRel(t, "sub"); !ok || !n.IsDir {
		t.Fatalf("server did not reflect mkdir of %q (ok=%v dir=%v)", "sub", ok, n.IsDir)
	}
	assertBaselineSynced(t, sc, "sub")

	// rename - moving the file under the new directory must move it server-side.
	sc.renameLocal(t, "file.txt", "sub/renamed.txt")
	waitForSync(t, sc.eng, ltsTimeout, func() bool {
		_, oldOK := sc.queryRel(t, "file.txt")
		n, newOK := sc.queryRel(t, "sub/renamed.txt")
		return !oldOK && newOK && !n.IsDir
	})
	if _, ok := sc.queryRel(t, "file.txt"); ok {
		t.Fatalf("server still has the pre-rename path %q", "file.txt")
	}
	assertServerContent(t, sc, "sub/renamed.txt", modified)
	assertBaselineSynced(t, sc, "sub/renamed.txt")

	// remove - deleting the renamed file locally must delete it server-side.
	sc.removeLocal(t, "sub/renamed.txt")
	waitForSync(t, sc.eng, ltsTimeout, func() bool {
		_, ok := sc.queryRel(t, "sub/renamed.txt")
		return !ok
	})
	if _, ok := sc.queryRel(t, "sub/renamed.txt"); ok {
		t.Fatalf("server still has the removed path %q", "sub/renamed.txt")
	}
	// The surviving directory's baseline stays synced.
	assertBaselineSynced(t, sc, "sub")
}

// waitServerHash polls until the server node at a scratch-relative path has the MD4
// of want, nudging the engine each round. Comparing the server-reported hash avoids
// downloading the body on every poll; the caller confirms the bytes once it holds.
func (sc *scenario) waitServerHash(t *testing.T, rel string, want []byte) {
	t.Helper()
	wantHash := md4.ContentHash(want)
	waitForSync(t, sc.eng, ltsTimeout, func() bool {
		n, ok := sc.queryRel(t, rel)
		return ok && !n.IsDir && n.Hash == wantHash
	})
}

// assertServerContent downloads the server node at a scratch-relative path and
// fails unless its bytes equal want.
func assertServerContent(t *testing.T, sc *scenario, rel string, want []byte) {
	t.Helper()
	n, ok := sc.queryRel(t, rel)
	if !ok {
		t.Fatalf("server is missing %q", rel)
	}
	got := serverRead(t, sc.client, sc.view.RootNodeID, n.FileID)
	if !bytes.Equal(got, want) {
		t.Fatalf("server content for %q mismatch: got %d bytes, want %d bytes", rel, len(got), len(want))
	}
}

// assertBaselineSynced fails unless the local store baseline for a scratch-relative
// path exists and has settled to StateSynced.
// assertBaselineSynced WAITS for the baseline to settle rather than sampling it
// once. The content landing on disk (or on the server) and the baseline being
// marked synced are two separate commits: the apply promotes the bytes first and
// markSynced records agreement after, so an immediate assertion races the engine
// and fails intermittently on whichever step the scheduler happens to split.
func assertBaselineSynced(t *testing.T, sc *scenario, rel string) {
	t.Helper()
	waitForSync(t, sc.eng, ltsTimeout, func() bool {
		st, present := sc.baselineState(t, rel)
		return present && st == store.StateSynced
	})
}
