//go:build integration

package integration

// Measures what the server answers when a CREATE names a parent that is a FILE.
//
// This is not a curiosity: it is the shape a file/directory clash puts a client in. Peer A
// makes `thing` a file; peer B makes `thing` a directory and writes `thing/inner.txt`. B's
// own mkdir for `thing` draws 24581 and is dead-lettered, correctly -- but B's upload of
// `thing/inner.txt` keeps asking for a parent that is a file, and whatever THAT answers
// decides whether the op retries forever. Since a conflicting op's successors are serialised
// behind it, an op that never settles wedges the inbound apply that would resolve the clash,
// and the two peers never converge.
//
// Measured, not guessed: the classifier must not be taught a code nobody watched the server
// send.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_CreateUnderAFileParent records the codes an upload and a mkdir draw when
// the parent node they name is a regular file.
func TestIntegration_CreateUnderAFileParent(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	// A real FILE to use as the (invalid) parent.
	parent := sc.remotePut(t, "fileparent/occupant", []byte("I am a file, not a directory\n"))
	t.Logf("occupant file: id=%q size=%d", parent.FileID, parent.Size)

	// Repeat the upload: a code the classifier may act on must be DETERMINISTIC, not one
	// answer from a momentarily unhappy server. Each attempt gets a fresh connection,
	// because the refusal is measured below to leave the turn desynced.
	for i := 0; i < 3; i++ {
		sc.redialClient(t)
		ctx, cancel := actionCtx()
		_, err := sc.client.Upload(ctx, parent.FileID, "child.txt",
			time.Now().Unix(), sc.view.RootNodeID, []byte("a child of a file\n"))
		cancel()
		reportCreateRefusal(t, fmt.Sprintf("upload #%d", i+1), err)

		// An upload is a TWO-frame turn (metadata, then body). Whether a refusal of the first
		// frame leaves the turn finished or the connection desynced decides what each retry of a
		// wedged upload actually costs the daemon -- a session, or nothing.
		if i == 0 {
			lctx, lcancel := actionCtx()
			_, _, lerr := sc.client.List(lctx, sc.view.ViewID, "/"+sc.viewRel("fileparent"))
			lcancel()
			if lerr != nil {
				t.Logf("MEASURED: an upload refusal leaves the connection unusable: %v", lerr)
			} else {
				t.Logf("MEASURED: the connection survives an upload refusal")
			}
		}
	}

	sc.redialClient(t)
	mctx, mcancel := actionCtx()
	defer mcancel()
	_, err := sc.client.Mkdir(mctx, parent.FileID, "childdir", time.Now().Unix(), sc.view.RootNodeID)
	reportCreateRefusal(t, "mkdir", err)

	// And the turn's aftermath: does a refused create leave the connection usable? The engine
	// retries this op every cycle, so a refusal that costs a session is a cost worth knowing.
	if _, _, lerr := sc.client.List(mctx, sc.view.ViewID, "/"+sc.viewRel("fileparent")); lerr != nil {
		t.Logf("MEASURED: the connection is unusable after a refused create: %v", lerr)
	} else {
		t.Logf("MEASURED: the connection survives a refused create")
	}
}

// reportCreateRefusal prints the verdict shape a refused create carries, and fails only if the
// server ACCEPTED the create -- the one outcome that would mean this whole class of clash needs
// rethinking rather than classifying.
func reportCreateRefusal(t *testing.T, verb string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("integration: %s under a FILE parent SUCCEEDED; the clash model assumes it cannot", verb)
	}
	var se *actions.ServerError
	if errors.As(err, &se) {
		t.Logf("MEASURED: %s under a file parent -> code %d (%#x)", verb, se.Code, se.Code)
	} else {
		t.Logf("MEASURED: %s under a file parent -> non-server error: %v", verb, err)
	}
	for _, c := range []struct {
		name     string
		sentinel error
	}{
		{"ErrPathError", actions.ErrPathError},
		{"ErrConflictVersion", actions.ErrConflictVersion},
		{"ErrNameCaseCollision", actions.ErrNameCaseCollision},
		{"ErrNameTypeCollision", actions.ErrNameTypeCollision},
		{"ErrNodeBusy", actions.ErrNodeBusy},
		{"ErrConnBroken", actions.ErrConnBroken},
		{"ErrUnexpectedReply", actions.ErrUnexpectedReply},
	} {
		if errors.Is(err, c.sentinel) {
			t.Logf("  %s classified as %s", verb, c.name)
		}
	}
}
