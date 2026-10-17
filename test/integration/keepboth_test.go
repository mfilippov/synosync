//go:build integration

package integration

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// keepBothTimeout bounds how long the cold-start keep-both resolution may take.
// It is generous because the trigger runs during the view's one-time cold-start
// full-catalog pull (DESIGN §6e), which paginates the whole server tree before the
// divergent path is reconciled.
const keepBothTimeout = 120 * time.Second

// TestIntegration_KeepBothColdStart drives the Phase-6d keep-both resolver
// end-to-end against a live DSM via the most deterministic trigger available:
// cold-start content divergence. Before the engine ever runs, the same view-relative
// path is given DIFFERENT content on the server (via the mutator client) and on the
// local sync root, so the view's first cold-start reconcile sees the path present on
// both sides with divergent hashes and funnels it through the keep-both resolver
// (server wins; the divergent local content is preserved as a _Conflict copy and
// pushed back).
//
// Post-conditions asserted:
//   - the canonical path on disk now holds the SERVER bytes (server wins);
//   - a durable store.Conflict record exists for the canonical path;
//   - the preserved _Conflict copy named by that record holds the original LOCAL
//     bytes (nothing was clobbered);
//   - the canonical baseline settled to StateSynced.
//
// It self-skips without the SYNOSYNC_IT_* env (see docs/INTEGRATION.md).
func TestIntegration_KeepBothColdStart(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	const rel = "report.txt"
	serverContent := []byte("keep-both: SERVER authoritative version, wins the canonical path\n")
	localContent := []byte("keep-both: LOCAL divergent edit, must survive as a _Conflict copy\n")

	// Place the divergent versions on both sides BEFORE the engine starts, so the
	// view's first cold-start reconcile observes the path present on both with
	// mismatched content rather than as a one-sided create.
	sc.remotePut(t, rel, serverContent)
	sc.writeLocal(t, rel, localContent)

	sc.start(t)

	// Wait until the conflict is resolved: a durable record exists for the canonical
	// path and the canonical file on disk now holds the server bytes.
	canonical := sc.viewRel(rel)
	var rec store.Conflict
	waitForSync(t, sc.eng, keepBothTimeout, func() bool {
		r, ok := sc.findConflict(t, canonical)
		if !ok {
			return false
		}
		got, exists := sc.readLocal(t, rel)
		if !exists || !bytes.Equal(got, serverContent) {
			return false
		}
		rec = r
		return true
	})

	// Server won: the canonical path holds the server bytes and its baseline synced.
	assertLocalContent(t, sc, rel, serverContent)
	assertBaselineSynced(t, sc, rel)

	// The divergent local content was preserved, not clobbered: the _Conflict copy
	// named by the record holds the original local bytes. CopyPath is view-relative
	// and includes the scratch base; strip it to the scratch-base-relative form
	// readLocal expects.
	if rec.CopyPath == "" {
		t.Fatalf("integration: conflict record for %q has no copy path", canonical)
	}
	copyRel := strings.TrimPrefix(rec.CopyPath, sc.base+"/")
	if copyRel == rec.CopyPath {
		t.Fatalf("integration: conflict copy path %q is not under scratch base %q", rec.CopyPath, sc.base)
	}
	copyContent, ok := sc.readLocal(t, copyRel)
	if !ok {
		t.Fatalf("integration: keep-both copy missing on disk at %q", copyRel)
	}
	if !bytes.Equal(copyContent, localContent) {
		t.Fatalf("integration: keep-both copy %q content mismatch: got %d bytes, want %d (the original local edit)",
			copyRel, len(copyContent), len(localContent))
	}
}

// findConflict returns the store.Conflict record for the given view-relative
// canonical path and whether it exists, scanning the view's conflict list.
func (sc *scenario) findConflict(t *testing.T, canonical string) (store.Conflict, bool) {
	t.Helper()
	recs, err := sc.st.ListConflicts(sc.viewID)
	if err != nil {
		t.Fatalf("integration: list conflicts for view %d: %v", sc.viewID, err)
	}
	for _, r := range recs {
		if r.CanonicalPath == canonical {
			return r, true
		}
	}
	return store.Conflict{}, false
}
