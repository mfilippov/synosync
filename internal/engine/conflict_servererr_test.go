package engine

// The keep-both resolver's terminal exit for a DETERMINISTIC unmapped server rejection.
//
// applyOp routes OpResolveConflict through its own early return, ahead of the outbound
// branch where escalateDeterministicServerFailure is wired. The resolver
// makes the same push-class turns (QueryNode/Download/Upload), but a persistent unmapped
// *actions.ServerError from any of them fell into applyResolveConflict's transient
// fall-through: retried every drain forever, never counted by noteServerErr, and - via
// blockedKeys - serialising the authoritative inbound remove/rename of the same path (and
// every overlapping descendant) behind it permanently.
//
// The escalation now wraps the resolve route too. Its terminal exit is the one this verb
// already uses for nameTooLong/errConflictServerGone: markOutboundConflict, then a durable
// trace recorded atomically with the ack.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// resolveServerErrFixture seeds the wedge shape these tests are about: a divergent local file at
// "a.txt" with a queued keep-both resolve, and an inbound OpRemove of the SAME path queued
// behind it (so blockedKeys serialises the authoritative server delete behind the resolver).
// The caller scripts how the server answers the resolver's QueryNode via the returned client.
func resolveServerErrFixture(t *testing.T) (*Engine, *store.Store, *pushClient, string, *bytes.Buffer) {
	t.Helper()
	localBytes := []byte("local-version-divergent")
	localHash := md4.ContentHash(localBytes)

	c := &pushClient{}
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	root := t.TempDir()
	viewDir := filepath.Join(root, "Photos")
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatalf("seed view dir: %v", err)
	}
	var logbuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	e := New(st, &concTransport{client: c}, root, "sess", log)
	pinClock(e, "laptop", time.Date(2026, 6, 11, 15, 30, 4, 0, time.UTC))

	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed local file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileType: "file", FileID: "srv1", LocalHash: localHash, ServerHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StateConflict,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpResolveConflict, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt", UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue resolve: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpRemove, Direction: store.DirInbound, ViewID: 7, Path: "a.txt", FileID: "srv1",
	}); err != nil {
		t.Fatalf("Enqueue inbound remove: %v", err)
	}
	return e, st, c, viewDir, &logbuf
}

// TestResolveConflictDeterministicServerErrnoDeadLetters is the red test: the server refuses
// the resolver's QueryNode with one and the same unmapped errno every drain. Pre-fix the
// resolve retried forever and the inbound remove queued behind it never ran. Now the identical
// rejections accumulate as determinism evidence, the resolve is dead-lettered with the CODE in
// its durable trace, and the remove - no longer blocked by a failing predecessor - drains in
// that very pass.
func TestResolveConflictDeterministicServerErrnoDeadLetters(t *testing.T) {
	e, st, c, viewDir, logbuf := resolveServerErrFixture(t)
	advance := stubClock(e)
	c.queryNodeErrByPath = map[string]error{"a.txt": &actions.ServerError{Code: 33333}}

	// Every drain below the streak leaves BOTH ops queued: an unknown code is not yet proof
	// of permanence, and the remove is skipped behind the still-failing resolve.
	for i := 0; i < unmappedServerErrDrains-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(unmappedServerErrMinAge)
		if n, _ := st.PendingLen(); n != 2 {
			t.Fatalf("drain %d: pending = %d, want both ops still queued", i+1, n)
		}
		if dl, _ := st.DeadLetterLen(); dl != 0 {
			t.Fatalf("drain %d: dead-lettered %d ops before the streak was proven", i+1, dl)
		}
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("escalation drain: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending after escalation = %d, want the resolve dropped and the remove unblocked", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || dls[0].Type != store.OpResolveConflict {
		t.Fatalf("dead letters = %+v, want exactly the dropped resolve-conflict op", dls)
	}
	if !strings.Contains(dls[0].Reason, "33333") {
		t.Errorf("resolve dead-letter trace = %q, want it to carry the server error code", dls[0].Reason)
	}
	// The server's authoritative delete applied once the resolver stopped blocking the path -
	// the starvation these tests are about.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("Lstat(a.txt) err = %v, want the inbound remove to have applied", err)
	}
	// The drop stays visible rather than vanishing: the divergence is recorded StateConflict
	// before the trace+ack (and the applied remove then drops the record, as for any delete).
	logs := logbuf.String()
	if !strings.Contains(logs, "deterministic") || !strings.Contains(logs, "33333") {
		t.Errorf("escalation Warn missing from the log:\n%s", logs)
	}
	if got := e.StuckOps(); got != 0 {
		t.Errorf("StuckOps after the chain drained = %d, want 0", got)
	}
}

// TestResolveConflictDeterministicServerErrnoMarksConflictBaseline pins the escalation's
// baseline step on the resolve route in isolation (no inbound remove to consume the record
// afterwards): the dropped resolution leaves the path recorded StateConflict, so it shows in
// `Unresolved` instead of being silently forgotten and re-detected as a fresh local create.
func TestResolveConflictDeterministicServerErrnoMarksConflictBaseline(t *testing.T) {
	e, st, c, viewDir, _ := resolveServerErrFixture(t)
	advance := stubClock(e)
	c.queryNodeErrByPath = map[string]error{"a.txt": &actions.ServerError{Code: 33333}}
	// Drop the inbound remove so nothing consumes the baseline after the escalation.
	ops, err := st.PendingOpsN(2)
	if err != nil {
		t.Fatalf("PendingOpsN: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("pending ops = %d, want 2", len(ops))
	}
	if err := st.Ack(ops[1].Seq); err != nil {
		t.Fatalf("Ack inbound remove: %v", err)
	}

	for i := 0; i < unmappedServerErrDrains; i++ {
		if err := e.drainQueue(context.Background()); err != nil && i == unmappedServerErrDrains-1 {
			t.Fatalf("escalation drain: %v", err)
		}
		advance(unmappedServerErrMinAge)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending = %d, want the resolve dropped", n)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateConflict {
		t.Errorf("baseline state = %v, want StateConflict", b.State)
	}
	// Data-safe: nothing on disk was touched - only the retry stopped.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); err != nil {
		t.Errorf("Lstat(a.txt) = %v, want the divergent local content untouched", err)
	}
}

// TestResolveConflictIDLessServerNodeDeadLetters covers the non-answer the resolver cannot act
// on: a query_node reply that describes a node but carries no file id. This is
// also reachable from a second direction - serverDivergedForVanishedUpload now reads a
// hash-carrying, id-less reply as divergence and funnels it here - so on a DSM that
// systematically omits file_id the resolver failed this way on EVERY drain, with no server
// result code for the escalation to count and blockedKeys holding the authoritative inbound
// remove behind it forever. An unbroken run of the identical non-answer is the same determinism
// evidence, so it takes the same terminal exit, with a trace an operator can act on.
func TestResolveConflictIDLessServerNodeDeadLetters(t *testing.T) {
	e, st, c, viewDir, _ := resolveServerErrFixture(t)
	advance := stubClock(e)
	// A node that answers for the path - a hash, a size - but no id at all.
	c.queryNode = map[string]actions.Node{"a.txt": {Hash: "bb22", Size: 9, MTime: 42}}

	for i := 0; i < unmappedServerErrDrains-1; i++ {
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the id-less reply refused", i+1)
		}
		advance(unmappedServerErrMinAge)
		if dl, _ := st.DeadLetterLen(); dl != 0 {
			t.Fatalf("drain %d: dead-lettered %d ops before the streak was proven", i+1, dl)
		}
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("escalation drain: %v", err)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Fatalf("pending after escalation = %d, want the resolve dropped and the remove unblocked", n)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || dls[0].Type != store.OpResolveConflict {
		t.Fatalf("dead letters = %+v, want exactly the dropped resolve-conflict op", dls)
	}
	if !strings.Contains(dls[0].Reason, "without a file id") {
		t.Errorf("dead-letter trace = %q, want it to name the missing file id rather than a bare code", dls[0].Reason)
	}
	// The inbound remove the resolver was blocking finally applied - the starvation this arm ends.
	if _, err := os.Lstat(filepath.Join(viewDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("Lstat(a.txt) err = %v, want the inbound remove to have applied", err)
	}
}

// TestResolveConflictIDLessThenAnsweredNeverEscalates is the conservatism guard on that arm: a
// single id-less reply is no proof of anything permanent (Node.FileID is tolerantly decoded), so
// a server that answers properly again resets the streak and the resolution runs.
func TestResolveConflictIDLessThenAnsweredNeverEscalates(t *testing.T) {
	e, st, c, _, _ := resolveServerErrFixture(t)
	advance := stubClock(e)

	for i := 0; i < 3*unmappedServerErrDrains; i++ {
		c.mu.Lock()
		if i%2 == 0 {
			c.queryNode = map[string]actions.Node{"a.txt": {Hash: "bb22", Size: 9}}
			c.queryNodeErrByPath = nil
		} else {
			// Any other failure class in between breaks the run of identical non-answers.
			c.queryNode = nil
			c.queryNodeErrByPath = map[string]error{"a.txt": errors.New("dial: connection refused")}
		}
		c.mu.Unlock()
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected failure", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on an interrupted streak, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued (an interrupted run is not proof)", n)
	}
}

// TestResolveConflictAlternatingServerErrnosStayTransient is the conservatism guard on the new
// route: determinism is the only evidence the drop rests on, so codes that change from drain to
// drain never escalate - the resolve keeps retrying (observably, via Stuck) exactly as before.
func TestResolveConflictAlternatingServerErrnosStayTransient(t *testing.T) {
	e, st, c, _, _ := resolveServerErrFixture(t)
	advance := stubClock(e)

	for i := 0; i < 3*unmappedServerErrDrains; i++ {
		code := int64(33333)
		if i%2 == 0 {
			code = 44444
		}
		c.mu.Lock()
		c.queryNodeErrByPath = map[string]error{"a.txt": &actions.ServerError{Code: code}}
		c.mu.Unlock()
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the injected server rejection", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on alternating codes, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 2 {
		t.Errorf("pending = %d, want both ops still queued (alternating codes are not proof)", n)
	}
}

// TestResolveConflictMappedServerErrnosNeverEscalate pins the mapped codes out of the new
// route: 28672 (ErrPathError) and 24579 (ErrConflictVersion) have typed sentinels and their own
// arcs inside resolveConflict - an inconclusive absence probe for the former, a plain retry for
// the latter. ServerErrorCode is false for both, so no amount of repetition may fold them into
// the "unknown deterministic rejection" class and drop the resolution.
func TestResolveConflictMappedServerErrnosNeverEscalate(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		// listErr keeps the ErrPathError arc's absence probe inconclusive, so the op stays
		// queued through its own path rather than dead-lettering as errConflictServerGone.
		listErr error
	}{
		{name: "path-error", err: actions.ErrPathError, listErr: errors.New("dial: connection refused")},
		{name: "stale-base", err: actions.ErrConflictVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, st, c, _, _ := resolveServerErrFixture(t)
			advance := stubClock(e)
			c.queryNodeErrByPath = map[string]error{"a.txt": tc.err}
			c.listErr = tc.listErr

			for i := 0; i < 2*unmappedServerErrDrains; i++ {
				if err := e.drainQueue(context.Background()); err == nil {
					t.Fatalf("drain %d: succeeded, want the injected rejection", i+1)
				}
				advance(unmappedServerErrMinAge)
			}
			if dl, _ := st.DeadLetterLen(); dl != 0 {
				t.Errorf("dead-lettered %d ops on a MAPPED code, want 0 (it has its own arc)", dl)
			}
			if n, _ := st.PendingLen(); n != 2 {
				t.Errorf("pending = %d, want both ops still queued", n)
			}
		})
	}
}
