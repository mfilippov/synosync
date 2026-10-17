//go:build integration

package integration

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
)

// TestIntegration_UploadConflictSemantics measures what the server's
// `conflict_policy: "compare_mtime"` (actions/upload.go uploadParams) actually
// enforces when an upload lands on a node whose content changed since the client
// last agreed with it.
//
// This is the ONLY divergence check the outbound path has: the upload request
// carries no base version - max_id and sync_id are sent as 0 - so whatever the
// server decides from the mtime alone is the whole of the client's protection
// against overwriting another client's edit. The engine's keep-both funnel for an
// outbound modify hangs off the 24579 rejection this test characterises
// (engine/outbound.go funnelUploadConflict), so which side of the comparison
// rejects and which silently overwrites decides whether a two-writer race is
// preserved or lost.
//
// It drives the raw action client only - no engine - so the measurement is of the
// server, not of the sync loop.
func TestIntegration_UploadConflictSemantics(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	parent := sc.remoteMkdirAll(t, "conflict")
	const name = "x.txt"

	v1 := []byte("upload-conflict: version 1, the agreed base\n")
	v2 := []byte("upload-conflict: version 2, written with a NEWER mtime\n")
	v3 := []byte("upload-conflict: version 3, written with an OLDER mtime\n")
	v4 := []byte("upload-conflict: version 4, written with the SAME mtime\n")

	base := time.Now().Unix()

	// The agreed base: a node with a known mtime.
	ctx, cancel := actionCtx()
	n1, err := sc.client.Upload(ctx, parent.FileID, name, base, sc.view.RootNodeID, v1)
	cancel()
	if err != nil {
		t.Fatalf("integration: upload v1: %v", err)
	}
	t.Logf("v1 landed: hash=%s mtime=%d size=%d", n1.Hash, n1.MTime, n1.Size)

	// A NEWER mtime: this is the shape a second client's edit takes - it never saw
	// v1's successor and simply writes its own file, whose mtime is later.
	ctx, cancel = actionCtx()
	n2, err2 := sc.client.Upload(ctx, parent.FileID, name, base+600, sc.view.RootNodeID, v2)
	cancel()
	//
	// This is the measurement the branch's whole H1 precondition is built on, so it is ASSERTED,
	// not logged: guardUploadTarget exists precisely BECAUSE the server takes this write, and the
	// unit fakes model a server that does. A DSM that started rejecting it would make every
	// keep-both funnel test rest on an input production no longer produces - the same "a fake must
	// never be kinder than the measured server" failure, inverted.
	switch {
	case err2 == nil:
		t.Logf("MEASURED (as expected): NEWER mtime ACCEPTED - the node now holds v2 (hash=%s mtime=%d)",
			n2.Hash, n2.MTime)
	case errors.Is(err2, actions.ErrConflictVersion):
		t.Fatalf("integration: a NEWER-mtime overwrite was REJECTED with 24579. compare_mtime now " +
			"refuses the two-writer race the client proves itself (guardUploadTarget), so the " +
			"pre-turn probe's premise and every unit fake built on it are stale")
	default:
		t.Fatalf("integration: upload v2 failed with an unexpected error: %v", err2)
	}
	assertNodeContent(t, sc, sc.viewRel("conflict/"+name), v2)

	// An OLDER mtime than the node now carries.
	ctx, cancel = actionCtx()
	_, err3 := sc.client.Upload(ctx, parent.FileID, name, base-600, sc.view.RootNodeID, v3)
	cancel()
	// The other side of the same rule, and the ONLY input that produces the 24579 the keep-both
	// funnel was originally written for: "compare_mtime accepts whenever the request's mtime is
	// not OLDER than the node's". Asserted for the same reason as the newer-mtime arm.
	switch {
	case errors.Is(err3, actions.ErrConflictVersion):
		t.Logf("MEASURED (as expected): OLDER mtime REJECTED with 24579 (the engine funnels this to keep-both)")
	case err3 == nil:
		t.Fatalf("integration: an OLDER-mtime upload was ACCEPTED - compare_mtime no longer rejects " +
			"anything at all, so 24579 has no producer and funnelUploadConflict's second line is dead")
	default:
		t.Fatalf("integration: upload v3 failed with an unexpected error: %v", err3)
	}

	// A rejected upload leaves the client broken by contract (the body frame was
	// already in flight), so re-dial before measuring anything else.
	if err3 != nil {
		sc.redialClient(t)
	}

	// The SAME mtime the node currently carries - the boundary of the comparison.
	cur, ok := serverQuery(t, sc.client, sc.view.ViewID, sc.viewRel("conflict/"+name))
	if !ok {
		t.Fatalf("integration: the node vanished mid-measurement")
	}
	ctx, cancel = actionCtx()
	_, err4 := sc.client.Upload(ctx, parent.FileID, name, cur.MTime, sc.view.RootNodeID, v4)
	cancel()
	// The boundary of "not OLDER": equal mtimes are accepted, which is what makes the acceptance
	// rule a >= rather than a >.
	switch {
	case err4 == nil:
		t.Logf("MEASURED (as expected): SAME mtime ACCEPTED - the node now holds v4")
	case errors.Is(err4, actions.ErrConflictVersion):
		t.Fatalf("integration: an equal-mtime upload was REJECTED with 24579 - the acceptance rule " +
			"is no longer \"not older\"; re-derive it before trusting the pre-turn probe's shape")
	default:
		t.Fatalf("integration: upload v4 failed with an unexpected error: %v", err4)
	}

	t.Logf("MEASURED: compare_mtime accepts a newer-mtime overwrite unconditionally - " +
		"the server performs NO base-version check, so an outbound modify would destroy " +
		"a concurrent edit the client never saw were it not for guardUploadTarget " +
		"(see TestIntegration_TwoClientsConcurrentEdit)")
}

// assertNodeContent downloads a view-relative server path and fails unless its bytes
// equal want. It is the engine-free counterpart of assertServerContent (which is
// scratch-relative and used by the engine scenarios).
func assertNodeContent(t *testing.T, sc *scenario, viewRel string, want []byte) {
	t.Helper()
	n, ok := serverQuery(t, sc.client, sc.view.ViewID, viewRel)
	if !ok {
		t.Fatalf("integration: server is missing %q", viewRel)
	}
	got := serverRead(t, sc.client, sc.view.RootNodeID, n.FileID)
	if !bytes.Equal(got, want) {
		t.Fatalf("integration: server content for %q mismatch: got %d bytes (hash %s), want %d bytes (hash %s)",
			viewRel, len(got), md4.ContentHash(got), len(want), md4.ContentHash(want))
	}
}
