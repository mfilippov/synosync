package engine

// Tests for errDownloadNodeIsDir: the apply-time backstop that converts an inbound OpDownload the
// SERVER places at a DIRECTORY into an inbound OpMkdir at the same path.
//
// The shape it closes is compensatedKind's declared residual. A dropped inbound rename whose
// moved node no surviving id-matched baseline can identify types its H2 repair from the move
// event's own is_dir, and a sparse move row reports false whatever the node really is - so a
// dropped DIRECTORY rename re-requests the node as a download. The download verb addresses
// "id:<file id>" and can never materialise a directory; inbound escalation is deliberately closed
// (servererr.go), so before this backstop the op retried every drain forever with every
// path-overlapping op wedged behind it on blockedKeys.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/store"
)

// TestDownloadOfDirectoryNodeConvertsToMkdir is the whole arc: the download turn fails with a
// server verdict, the kind probe places the node as a directory under the op's own file id, the
// op leaves the queue with a durable trace, and the compensating OpMkdir it was replaced with
// applies - materialising the directory the download could never have made.
func TestDownloadOfDirectoryNodeConvertsToMkdir(t *testing.T) {
	c := &pushClient{
		downloadErr: actions.ErrPathError,
		// The server places the node: is_dir positively set AND the id matches the op's.
		queryNode: map[string]actions.Node{
			"photos": {Name: "photos", FileID: "fdir", IsDir: true},
		},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "photos", FileID: "fdir",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// A replaced op is a resolved op, not a drain failure. It leaves the queue holding its path
	// key for the rest of the pass (errOpReplaced takes the still-queued treatment, so nothing
	// overlapping the path applies past a node that has not landed yet), so the compensating
	// mkdir it enqueued applies on the NEXT pass - which loop.go runs twice per cycle anyway.
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue of the doomed download: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue of the compensating mkdir: %v", err)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen = %d (err %v), want the queue drained (0)", n, err)
	}

	// The repair actually materialises the node - as a DIRECTORY, which is the whole point.
	fi, err := os.Lstat(filepath.Join(viewDir, "photos"))
	if err != nil {
		t.Fatalf("Lstat repaired node: %v", err)
	}
	if !fi.IsDir() {
		t.Error("repaired node is not a directory")
	}
	// And it is recorded as agreement under the moved node's own id, so the next scan neither
	// re-uploads it nor diffs it as a local-only create.
	base, err := st.GetBaseline(7, "photos")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if base.FileID != "fdir" || base.State != store.StateSynced {
		t.Errorf("baseline = {id %q, state %v}, want the moved id synced", base.FileID, base.State)
	}

	// The drop is observable: a durable trace naming both what failed and what replaced it.
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters = %d, want exactly 1", len(dls))
	}
	if !strings.Contains(dls[0].Reason, "directory") || !strings.Contains(dls[0].Reason, "mkdir") {
		t.Errorf("trace reason = %q, want it to name the directory verdict and the mkdir repair", dls[0].Reason)
	}
}

// TestDownloadOfDirectoryConversionRequiresPositiveEvidence pins both evidence axes. The
// conversion writes a DIRECTORY under the op's file id, so a non-answer must never reach it: a
// sparse reply (is_dir omitted), a foreign directory standing at the path, an unidentified
// directory (file_id omitted - decodeNode tolerates that) and a probe that itself fails all leave
// the op exactly where it was, queued and retrying on its historical transient fate.
//
// The download fails with an unmapped *ServerError rather than the path-error sentinel so the
// sibling node-gone classifier (gated on ErrPathError) never runs and only the kind axis is under
// test - and so the probe is shown to be spent on a typed result code too, not just on 28672.
func TestDownloadOfDirectoryConversionRequiresPositiveEvidence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		probe actions.Node
		perr  error
	}{
		{name: "sparse reply omits is_dir", probe: actions.Node{Name: "photos", FileID: "fdir"}},
		{name: "foreign directory occupies the path", probe: actions.Node{Name: "photos", FileID: "other", IsDir: true}},
		{name: "directory without an id", probe: actions.Node{Name: "photos", IsDir: true}},
		{name: "probe answers path-error", perr: actions.ErrPathError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{
				downloadErr: &actions.ServerError{Code: 33333},
				queryNode:   map[string]actions.Node{"photos": tc.probe},
			}
			if tc.perr != nil {
				c.queryNodeErrByPath = map[string]error{"photos": tc.perr}
			}
			e, st, _ := outboundEngine(t, c, "Photos", "root1")

			if _, err := st.Enqueue(store.Op{
				Type: store.OpDownload, Direction: store.DirInbound,
				ViewID: 7, Path: "photos", FileID: "fdir",
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			if err := e.drainQueue(context.Background()); err == nil {
				t.Fatal("drainQueue succeeded, want the download to keep failing")
			}

			pending, err := st.PendingOpsN(10)
			if err != nil {
				t.Fatalf("PendingOpsN: %v", err)
			}
			if len(pending) != 1 || pending[0].Type != store.OpDownload {
				t.Fatalf("pending = %+v, want the original download still queued", pending)
			}
			dls, err := st.ListDeadLetters()
			if err != nil {
				t.Fatalf("ListDeadLetters: %v", err)
			}
			if len(dls) != 0 {
				t.Errorf("dead letters = %d, want none: an unproved kind must not convert", len(dls))
			}
		})
	}
}

// TestDownloadKindProbeSkippedWithoutAnID pins the probe's identity precondition: an id-less
// download can be neither identified by the probe nor converted on its answer, so no probe turn
// is spent and the op keeps its historical transient fate.
func TestDownloadKindProbeSkippedWithoutAnID(t *testing.T) {
	c := &pushClient{
		downloadErr: actions.ErrPathError,
		queryNode:   map[string]actions.Node{"photos": {Name: "photos", FileID: "fdir", IsDir: true}},
	}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")

	if _, err := st.Enqueue(store.Op{
		Type: store.OpDownload, Direction: store.DirInbound,
		ViewID: 7, Path: "photos",
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err == nil {
		t.Fatal("drainQueue succeeded, want the id-less download to keep failing")
	}
	c.mu.Lock()
	calls := append([]string(nil), c.queryNodeCalls...)
	c.mu.Unlock()
	if len(calls) != 0 {
		t.Errorf("query_node calls = %v, want none for an id-less download", calls)
	}
	if n, err := st.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen = %d (err %v), want the id-less download still queued (1)", n, err)
	}
}

// TestServerRejectedDownloadClassifies pins which failures are worth a probe turn: a server
// VERDICT (a typed result code or the shared path-error sentinel the absence provers consume),
// and nothing else. A local failure or a transport blip says nothing about the node.
func TestServerRejectedDownloadClassifies(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "path error", err: actions.ErrPathError, want: true},
		{name: "server error", err: &actions.ServerError{Code: 32772}, want: true},
		{name: "connection broken", err: actions.ErrConnBroken},
		{name: "transfer stalled", err: actions.ErrTransferStalled},
		{name: "nil", err: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := serverRejectedDownload(tc.err); got != tc.want {
				t.Errorf("serverRejectedDownload(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestReplacedDownloadSignalsAreClassifiedApart pins the two typed signals the conversion rides
// on, both of which are load-bearing by their errors.Is relationships alone.
//
// downloadIsDirAt must still MATCH the bare sentinel, so every existing classifier keeps working
// while the resolved path travels with it. errOpReplaced must NOT match errDeadLettered: that
// signal means "left the queue without placing its node" and arms the cross-window doom, which
// drops a same-node inbound rename permanently - over a node that is not missing at all, merely
// still queued as the compensating mkdir.
func TestReplacedDownloadSignalsAreClassifiedApart(t *testing.T) {
	if !errors.Is(&downloadIsDirAt{path: "photos"}, errDownloadNodeIsDir) {
		t.Error("downloadIsDirAt no longer matches errDownloadNodeIsDir; every classifier of the bare sentinel goes blind")
	}
	if !strings.Contains((&downloadIsDirAt{path: "photos"}).Error(), "photos") {
		t.Error("downloadIsDirAt's message does not name the path it resolved")
	}
	if errors.Is(errOpReplaced, errDeadLettered) {
		t.Error("errOpReplaced matches errDeadLettered; a replaced op would arm the doom and drop a dependent rename")
	}
}
