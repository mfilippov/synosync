package engine

// Tests for the outbound upload divergence precondition (two-client finding H1,
// measured 14.08.2026; docs/INVARIANTS.md "upload guard").
//
// The fake's Upload SUCCEEDS in every divergence case below. That is the whole point: the live
// DSM's "compare_mtime" policy accepts a write whose mtime is not older than the node's, so the
// 24579 the engine used to wait for never arrives for an ordinary offline edit
// (TestIntegration_UploadConflictSemantics). A fake that returns ErrConflictVersion is KINDER than
// the measured server, and every keep-both funnel tested against it was tested against an input
// production almost never sees (§7). Here the server takes the write happily - so if the client
// pushes at all, the peer's version is gone.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// TestOutboundUploadModifyDivergedFunnelsWithoutPushing is the red test for finding H1's primary
// case: two clients edit one file. The baseline agrees with content X; a peer has since committed
// Y; the local edit is queued for push and the server would ACCEPT it. The push must not happen -
// the divergence must funnel into keep-both, with the server's version on the canonical path and
// the local bytes preserved as a _Conflict copy.
func TestOutboundUploadModifyDivergedFunnelsWithoutPushing(t *testing.T) {
	ts := time.Date(2026, 8, 14, 14, 25, 5, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("the-peer-committed-this-while-we-were-offline")
	localBytes := []byte("the-local-offline-edit")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("doc.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		// No uploadErr, no uploadConflictLeft: this server takes every push. Only the client's
		// own precondition stands between the local edit and the peer's version.
		queryNode: map[string]actions.Node{
			"doc.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode: map[string]actions.Node{
			"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		uploadNode: map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The last agreement was on "agreed-base-hash"; the server has moved off it.
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "agreed-base-hash", LocalHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "doc.txt",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(localBytes)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	// The peer's version survives on the canonical path; the local edit survives as a copy.
	if got, err := os.ReadFile(filepath.Join(viewDir, "doc.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server version %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the local bytes %q", got, err, localBytes)
	}
	// The canonical push never reached the server: the ONLY upload is the keep-both copy.
	if len(c.uploads) != 1 || c.uploads[0].name != copyName {
		t.Fatalf("uploads = %+v, want only the keep-both copy %q (the canonical push must never happen)",
			c.uploads, copyName)
	}
	for _, u := range c.uploads {
		if string(u.content) == string(localBytes) && u.name == "doc.txt" {
			t.Fatalf("the local edit was pushed over the peer's version: %+v", u)
		}
	}
	cb, err := st.GetBaseline(7, "doc.txt")
	if err != nil {
		t.Fatalf("GetBaseline canonical: %v", err)
	}
	if cb.State != store.StateSynced || cb.FileID != "srv1" || cb.LocalHash != serverHash {
		t.Errorf("canonical baseline = %+v, want synced srv1/%s", cb, serverHash)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "doc.txt" {
		t.Fatalf("conflicts = %+v, want one durable record for doc.txt", recs)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("queue not drained: %d ops remain", n)
	}
}

// TestOutboundUploadModifyDivergedWithBlankedServerHashFunnels composes the two halves of the
// H1-inertness fix end to end, which no other test does: the record's ServerHash is EMPTY and the
// divergence base has to come from the op's own reconcile-time capture.
//
// That is not a hypothetical shape. applyUploadOut assigns the upload reply's hash to ServerHash
// verbatim, so a DSM whose reply omits it BLANKS the field on every successful push - from then on
// the record can never supply a base again. With no base the guard degrades to node IDENTITY,
// which a peer's overwrite of the SAME node always satisfies, so the push proceeds and the peer's
// version is destroyed: H1 itself, not a weaker form of it. Op.BaseLocalHash is what keeps the
// guard live, and the drain is the only level at which planUpload's capture, uploadDivergenceBase's
// fallback and guardUploadTarget's comparison are all in the same run.
func TestOutboundUploadModifyDivergedWithBlankedServerHashFunnels(t *testing.T) {
	ts := time.Date(2026, 8, 14, 15, 5, 9, 0, time.UTC)
	const host = "laptop"

	agreedBytes := []byte("the-version-both-clients-last-agreed-on")
	serverBytes := []byte("the-peer-committed-this-while-we-were-offline")
	localBytes := []byte("the-local-offline-edit")
	agreedHash := md4.ContentHash(agreedBytes)
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("doc.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"doc.txt": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode: map[string]actions.Node{
			"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		uploadNode: map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "doc.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// The record carries NO ServerHash - the state a hash-omitting upload reply leaves behind -
	// and its LocalHash is already planUpload's stamp of the un-pushed edit, so reading the base
	// from the record would compare the new bytes with themselves.
	if err := st.PutBaseline(7, "doc.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "doc.txt",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(localBytes)), UnixPerm: 644,
		// planUpload's capture of the SYNCED record's LocalHash, taken before the stamp above.
		BaseLocalHash: agreedHash,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if len(c.uploads) != 1 || c.uploads[0].name != copyName {
		t.Fatalf("uploads = %+v, want only the keep-both copy %q; the guard went inert on the blanked ServerHash",
			c.uploads, copyName)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "doc.txt")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server version %q", got, err, serverBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the local bytes %q", got, err, localBytes)
	}
	recs, err := st.ListConflicts(7)
	if err != nil {
		t.Fatalf("ListConflicts: %v", err)
	}
	if len(recs) != 1 || recs[0].CanonicalPath != "doc.txt" {
		t.Fatalf("conflicts = %+v, want one durable record for doc.txt", recs)
	}
	if n, err := st.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want an empty queue", n, err)
	}
}

// TestOutboundUploadCreateOntoOccupiedPathFunnels is the red test for the SameNameCreate half of
// finding H1: both clients create the same path. The returning client's create carries no file id
// and no base, so nothing but occupancy can speak - and a server that accepts the write would
// simply replace the peer's node. The create must funnel into keep-both instead.
func TestOutboundUploadCreateOntoOccupiedPathFunnels(t *testing.T) {
	ts := time.Date(2026, 8, 14, 14, 25, 5, 0, time.UTC)
	const host = "laptop"

	peerBytes := []byte("the-peer-created-this-first")
	localBytes := []byte("we-created-the-same-name")
	peerHash := md4.ContentHash(peerBytes)
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("same.txt", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"same.txt": {FileID: "peer1", Hash: peerHash, MTime: 900, Size: int64(len(peerBytes))},
		},
		downloadContent: map[string][]byte{"peer1": peerBytes},
		downloadNode: map[string]actions.Node{
			"peer1": {FileID: "peer1", Hash: peerHash, MTime: 900, Size: int64(len(peerBytes))},
		},
		uploadNode: map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)

	if err := os.WriteFile(filepath.Join(viewDir, "same.txt"), localBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "same.txt", store.Baseline{
		FileType: "file", LocalHash: localHash, LocalSize: int64(len(localBytes)),
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "same.txt",
		LocalHash: localHash, LocalSize: int64(len(localBytes)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}

	if got, err := os.ReadFile(filepath.Join(viewDir, "same.txt")); err != nil || string(got) != string(peerBytes) {
		t.Fatalf("canonical content = %q (err %v), want the peer's node %q", got, err, peerBytes)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the local bytes %q", got, err, localBytes)
	}
	if len(c.uploads) != 1 || c.uploads[0].name != copyName {
		t.Fatalf("uploads = %+v, want only the keep-both copy (the create must never overwrite the peer)", c.uploads)
	}
}

// TestOutboundUploadResumePathTakesTheSameGuard pins that the LARGE-file path (finding H1's
// LargeFileConcurrentEdit, which is where resume_upload runs) is covered by the same precondition:
// the guard sits ahead of the small-vs-resume decision, so a diverged target funnels before a
// single byte is streamed.
func TestOutboundUploadResumePathTakesTheSameGuard(t *testing.T) {
	ts := time.Date(2026, 8, 14, 14, 25, 5, 0, time.UTC)
	const host = "laptop"

	serverBytes := []byte("peer-version-of-the-large-file")
	localBytes := []byte("this-local-body-is-over-the-resume-threshold")
	serverHash := md4.ContentHash(serverBytes)
	localHash := md4.ContentHash(localBytes)
	copyRel := conflictCopyName("big.bin", host, ts)
	copyName := filepath.Base(copyRel)

	c := &pushClient{
		queryNode: map[string]actions.Node{
			"big.bin": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		downloadContent: map[string][]byte{"srv1": serverBytes},
		downloadNode: map[string]actions.Node{
			"srv1": {FileID: "srv1", Hash: serverHash, MTime: 555, Size: int64(len(serverBytes))},
		},
		uploadNode:  map[string]actions.Node{copyName: {FileID: "copyf", Hash: localHash, MTime: 777}},
		resumeToken: actions.ResumeToken{Token: "tok"},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	pinClock(e, host, ts)
	e.limits.UploadResumeThreshold = 8 // force the resume path for this small test body

	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), localBytes, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "big.bin", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "agreed-base-hash", LocalHash: localHash,
		LocalSize: int64(len(localBytes)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(localBytes)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	// Exactly one body was streamed - the keep-both COPY the resolver pushes back (over the same
	// lowered threshold). The canonical body never left the disk, which is the point: had the
	// guard missed it, resumeChunks would carry it too and resumeSpec would name big.bin.
	if len(c.resumeChunks) != len(localBytes) {
		t.Errorf("resume streamed %d bytes, want only the %d-byte keep-both copy (the canonical push must never stream)",
			len(c.resumeChunks), len(localBytes))
	}
	if c.resumeSpec.Name != copyName {
		t.Errorf("last resume upload named %q, want the keep-both copy %q", c.resumeSpec.Name, copyName)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, "big.bin")); err != nil || string(got) != string(serverBytes) {
		t.Fatalf("canonical content = %q (err %v), want the server version", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(viewDir, copyName)); err != nil || string(got) != string(localBytes) {
		t.Fatalf("copy content = %q (err %v), want the local bytes", got, err)
	}
}

// TestOutboundUploadAgreedTargetPushesWithOneProbe covers the common (non-diverged) case: an
// agreeing target costs exactly ONE extra server turn - the probe - and the push proceeds and
// records normally. A guard that cost more than one turn, or that funnelled a matching target,
// would tax every ordinary modify.
func TestOutboundUploadAgreedTargetPushesWithOneProbe(t *testing.T) {
	content := []byte("the-new-local-content")
	localHash := md4.ContentHash(content)
	c := &pushClient{
		queryNode: map[string]actions.Node{
			"a.txt": {FileID: "srv1", Hash: "agreed-base-hash", MTime: 100, Size: 7},
		},
		uploadNode: map[string]actions.Node{"a.txt": {FileID: "srv1", Hash: localHash, MTime: 200}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "agreed-base-hash", LocalHash: localHash,
		LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := e.drainQueue(context.Background()); err != nil {
		t.Fatalf("drainQueue: %v", err)
	}
	if len(c.uploads) != 1 || string(c.uploads[0].content) != string(content) {
		t.Fatalf("uploads = %+v, want the agreed push to go through", c.uploads)
	}
	if len(c.queryNodeCalls) != 1 || c.queryNodeCalls[0] != "a.txt" {
		t.Errorf("QueryNode calls = %v, want exactly one probe of a.txt", c.queryNodeCalls)
	}
	b, err := st.GetBaseline(7, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.State != store.StateSynced || b.ServerHash != localHash {
		t.Errorf("baseline = %+v, want synced with the pushed content", b)
	}
	if n, _ := st.PendingLen(); n != 0 {
		t.Errorf("op not acked: %d remain", n)
	}
}

// TestOutboundUploadProbeInconclusiveLeavesOpQueued pins the fail-toward-"do not push" rule: a
// probe that neither answers nor reports the path-error sentinel decides nothing, so the op stays
// queued and NOTHING is sent. Deciding "still ours" from an unread server is exactly the default
// this precondition removes.
func TestOutboundUploadProbeInconclusiveLeavesOpQueued(t *testing.T) {
	content := []byte("the-new-local-content")
	localHash := md4.ContentHash(content)
	c := &pushClient{
		queryNodeErr: errors.New("server said something unhelpful"),
		uploadNode:   map[string]actions.Node{"a.txt": {FileID: "srv1", Hash: localHash}},
	}
	e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
	if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "agreed-base-hash", LocalHash: localHash,
		LocalSize: int64(len(content)), State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
		FileID: "srv1", LocalHash: localHash, LocalSize: int64(len(content)), UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	err := e.drainQueue(context.Background())
	if err == nil || !strings.Contains(err.Error(), "probe upload target") {
		t.Fatalf("drainQueue err = %v, want the inconclusive probe surfaced", err)
	}
	if len(c.uploads) != 0 {
		t.Errorf("uploads = %+v, want none on an undecided probe", c.uploads)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("PendingLen = %d, want the op still queued (1)", n)
	}
	if n, _ := st.DeadLetterLen(); n != 0 {
		t.Errorf("DeadLetterLen = %d, want an undecided probe NOT dead-lettered", n)
	}
}

// TestOutboundUploadProbePathErrorStillPushes pins the ErrPathError arm end to end, and it is
// NOT one arm: the sentinel maps both 32769 ("no such node") and 28672, which a live DSM also
// returns for a server-side timeout, so what a single one licenses depends on what the push would
// do with it.
//
//   - a CREATE reads it on the first probe. The sentinel is the ordinary answer for a path that is
//     free, and corroborating it would put absentNodeConfirmations drain passes and
//     absenceStreakMinAge of wall clock in front of EVERY new file.
//   - a MODIFY may not. "The node is gone, so there is nothing to overwrite" makes the whole H1
//     precondition inert on one timed-out probe, and the push that follows carries
//     conflict_policy "compare_mtime", which accepts any write whose mtime is not older - the peer's
//     version destroyed server-side and, once the overwrite echoes back, in both replicas. So the
//     absence is corroborated by repetition first (the same rule and the same streak map the
//     sibling directory-delete precondition uses at the root of its walk), and only then does the
//     ordinary push run with the post-turn machinery owning the outcome as before.
func TestOutboundUploadProbePathErrorStillPushes(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fileID empty marks a create, which pushes on the first probe.
		fileID string
		// probes is how many drains the push is expected to take before it runs.
		probes int
	}{
		{name: "create", fileID: "", probes: 1},
		{name: "modify whose node is gone", fileID: "srv1", probes: absentNodeConfirmations},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := []byte("hello")
			localHash := md4.ContentHash(content)
			c := &pushClient{
				queryNodeErr: actions.ErrPathError,
				uploadNode:   map[string]actions.Node{"a.txt": {FileID: "newf", Hash: localHash, MTime: 12}},
			}
			e, st, viewDir := outboundEngine(t, c, "Photos", "root1")
			now := time.Unix(1_700_000_000, 0)
			e.now = func() time.Time { return now }
			if err := os.WriteFile(filepath.Join(viewDir, "a.txt"), content, 0o644); err != nil {
				t.Fatalf("seed file: %v", err)
			}
			if err := st.PutBaseline(7, "a.txt", store.Baseline{
				FileID: tc.fileID, FileType: "file", ServerHash: "agreed-base-hash",
				LocalHash: localHash, LocalSize: int64(len(content)), State: store.StatePending,
			}); err != nil {
				t.Fatalf("seed baseline: %v", err)
			}
			if _, err := st.Enqueue(store.Op{
				Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "a.txt",
				FileID: tc.fileID, LocalHash: localHash, LocalSize: int64(len(content)), UnixPerm: 644,
			}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			// Every drain before the last must leave the op queued having pushed nothing: an
			// uncorroborated absence is undecided, and undecided never mutates the server.
			for i := 1; i < tc.probes; i++ {
				// Named, not merely non-nil: "some error" would also be satisfied by a store
				// failure or a transport break, and the point of this loop is that the op is
				// held by its own precondition rather than by an unrelated fault.
				if err := e.drainQueue(context.Background()); !errors.Is(err, errUploadTargetAbsenceUnproved) {
					t.Fatalf("drain #%d = %v, want errUploadTargetAbsenceUnproved; an uncorroborated absence must leave the op queued", i, err)
				}
				if len(c.uploads) != 0 {
					t.Fatalf("drain #%d pushed on one path-error probe: %+v", i, c.uploads)
				}
				if n, _ := st.PendingLen(); n != 1 {
					t.Fatalf("drain #%d left %d ops queued, want the push still owed", i, n)
				}
				// Past the streak's wall-clock floor, so the next probe counts as independent
				// evidence rather than as a second reading of one overload window.
				now = now.Add(absenceStreakMinAge)
			}
			if err := e.drainQueue(context.Background()); err != nil {
				t.Fatalf("drainQueue: %v", err)
			}
			if len(c.uploads) != 1 || string(c.uploads[0].content) != string(content) {
				t.Fatalf("uploads = %+v, want the push to proceed over a proved-absent target", c.uploads)
			}
			if n, _ := st.PendingLen(); n != 0 {
				t.Errorf("op not acked: %d remain", n)
			}
		})
	}
}

// TestGuardUploadTargetAbsenceCorroborationResets pins the resets that keep the corroboration
// honest, both inherited from absenceCorroboratedByNodeProbe: a probe that answers with anything
// other than the sentinel breaks the run, and a markSynced at the path does too. Without them a
// modify could have a LATER absence corroborated on one conflated 28672 riding on evidence
// gathered about a node that has since been replaced.
func TestGuardUploadTargetAbsenceCorroborationResets(t *testing.T) {
	const agreed = "agreed-base-hash"
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: "ours"}

	c := &pushClient{queryNodeErr: actions.ErrPathError}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	now := time.Unix(1_700_000_000, 0)
	e.now = func() time.Time { return now }

	// One sentinel: short of the verdict, so the push is refused.
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("one path-error probe allowed the push; the absence is uncorroborated")
	}

	// A probe that ANSWERS breaks the run. It also decides the guard on its own evidence - the
	// node is back and still holds the agreed base - so the push is allowed here for a reason
	// that has nothing to do with absence.
	c.queryNodeErr = nil
	c.queryNode = map[string]actions.Node{"a.txt": {FileID: "srv1", Hash: agreed, Size: 4}}
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err != nil {
		t.Fatalf("guardUploadTarget = %v, want the push allowed against the agreed base", err)
	}

	// The run restarts from zero: the next sentinel is a first probe, not a second one, even
	// though the wall-clock floor has long since elapsed.
	c.queryNodeErr = actions.ErrPathError
	now = now.Add(10 * absenceStreakMinAge)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("the sentinel after an answering probe completed a verdict; the run must restart")
	}
}

// TestGuardUploadTargetAbsenceResetByMarkSynced pins the OTHER reset the sibling above names but
// does not exercise: recording agreement at the path clears its streak. An apply that landed
// content there is the strongest possible refutation of "this node is gone", and a streak that
// outlived it would let one later 28672 complete a verdict about a node that has since been
// re-established.
func TestGuardUploadTargetAbsenceResetByMarkSynced(t *testing.T) {
	const agreed = "agreed-base-hash"
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: "ours"}

	c := &pushClient{queryNodeErr: actions.ErrPathError}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	now := time.Unix(1_700_000_000, 0)
	e.now = func() time.Time { return now }

	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("one path-error probe allowed the push; the absence is uncorroborated")
	}
	if err := e.markSynced(7, "a.txt", func(b *store.Baseline) {
		b.FileID, b.FileType = "srv1", "file"
		b.ServerHash, b.LocalHash, b.LocalSize = agreed, agreed, 4
	}); err != nil {
		t.Fatalf("markSynced: %v", err)
	}

	// A fresh pass, past the wall-clock floor: without the reset this second sentinel would be
	// the corroborating one and the push would run.
	now = now.Add(10 * absenceStreakMinAge)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); !errors.Is(err, errUploadTargetAbsenceUnproved) {
		t.Fatalf("guardUploadTarget = %v, want errUploadTargetAbsenceUnproved; markSynced must have cleared the streak", err)
	}
}

// TestGuardUploadTargetAbsenceHonoursTheWallClockFloor pins the floor the drain-level test cannot:
// that one advances `now` by absenceStreakMinAge on EVERY iteration, so deleting the floor check
// leaves it green. A pass count is not a duration - nothing spaces two drain passes in wall clock,
// and loop.go's leading and trailing drains land seconds apart inside ONE server overload window -
// so two sentinels gathered inside the floor are one reading, not two.
func TestGuardUploadTargetAbsenceHonoursTheWallClockFloor(t *testing.T) {
	const agreed = "agreed-base-hash"
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: "ours"}

	c := &pushClient{queryNodeErr: actions.ErrPathError}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	now := time.Unix(1_700_000_000, 0)
	e.now = func() time.Time { return now }

	if err := e.guardUploadTarget(context.Background(), c, op, agreed); !errors.Is(err, errUploadTargetAbsenceUnproved) {
		t.Fatalf("first probe = %v, want errUploadTargetAbsenceUnproved", err)
	}
	// A new drain pass, but only a moment later: the count advances, the clock does not.
	now = now.Add(absenceStreakMinAge - time.Second)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); !errors.Is(err, errUploadTargetAbsenceUnproved) {
		t.Fatalf("second probe inside the wall-clock floor = %v, want the verdict still unproved", err)
	}
	// Past the floor, the same probe completes the verdict.
	now = now.Add(2 * time.Second)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err != nil {
		t.Fatalf("probe past the wall-clock floor = %v, want the absence corroborated", err)
	}
}

// TestGuardUploadTargetMatrix pins the guard's decision table directly, including the arms an
// end-to-end drain cannot reach cheaply: the evidence rules that keep it from funnelling on
// nothing, and the one adoption it makes (the server already holds the very bytes this op set out
// to push - our own lost reply, not a peer's file).
func TestGuardUploadTargetMatrix(t *testing.T) {
	const (
		agreed  = "agreed-base-hash"
		peerNew = "peer-hash"
		ours    = "our-pushed-hash"
	)
	tests := []struct {
		name     string
		op       store.Op
		base     string
		node     actions.Node
		nodeErr  error
		wantErr  error
		wantPass bool
	}{
		{
			name: "modify: server moved off the agreed base",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: agreed, node: actions.Node{FileID: "srv1", Hash: peerNew, Size: 4},
			wantErr: errUploadTargetDiverged,
		},
		{
			name: "modify: a different node took the path over",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: agreed, node: actions.Node{FileID: "other", Hash: agreed, Size: 4},
			wantErr: errUploadTargetDiverged,
		},
		{
			name: "modify: server still holds the agreed base",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: agreed, node: actions.Node{FileID: "srv1", Hash: agreed, Size: 4},
			wantPass: true,
		},
		{
			name: "modify: server already holds the bytes we are pushing (our lost reply)",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: agreed, node: actions.Node{FileID: "srv1", Hash: ours, Size: 4},
			wantPass: true,
		},
		{
			name: "modify: no hash either side degrades to identity",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: "", node: actions.Node{FileID: "srv1", Size: 4},
			wantPass: true,
		},
		{
			// Half of the degradation, the other way round: the SERVER reports content but the
			// base is blank (a DSM that omits the hash from its upload reply blanks ServerHash on
			// every successful push, and op.BaseLocalHash was empty too). A present server hash
			// against an empty base must not read as divergence, or every push would funnel.
			name: "modify: a server hash against a blank base degrades to identity",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: "", node: actions.Node{FileID: "srv1", Hash: peerNew, Size: 4},
			wantPass: true,
		},
		{
			// And the mirror: a base is known, the server's reply omits the hash. Identity is all
			// that is left, and the same node is the unchanged one.
			name: "modify: a blank server hash against a known base degrades to identity",
			op:   store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base: agreed, node: actions.Node{FileID: "srv1", Size: 4},
			wantPass: true,
		},
		{
			// The arm where a destructive overwrite would proceed on ZERO server evidence. A
			// tolerant `file: {}` decodes exactly like an absent node, so it says nothing about
			// occupancy - for a MODIFY as much as for a create - and reading it as "a node is
			// there" would funnel every push into keep-both.
			name:     "modify: a bare reply asserts nothing and is not occupancy",
			op:       store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base:     agreed,
			node:     actions.Node{},
			wantPass: true,
		},
		{
			// An ID-LESS MODIFY. Against a DSM that omits file_id every synced record is id-less
			// (the shape removeTargetFacts was reshaped for), so this is the ordinary edit of an
			// ordinary file, not an exotic one. Classified by the id alone it took the CREATE arm:
			// the node it is about to update was of course "occupying the path", so every save
			// funnelled into keep-both - the local edit set aside as a _Conflict copy and the
			// server's older bytes downloaded back onto the canonical path, on every save. The op's
			// reconcile-time base capture is what tells the two classes apart.
			name: "modify: id-less, server still holds the agreed base",
			op:   store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours, BaseLocalHash: agreed},
			base: agreed, node: actions.Node{FileID: "srv1", Hash: agreed, Size: 4},
			wantPass: true,
		},
		{
			// The same op against a peer's commit: the H1 protection the misclassification was
			// hiding must still fire, on CONTENT - the evidence an id-less op does have.
			name: "modify: id-less, server moved off the agreed base",
			op:   store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours, BaseLocalHash: agreed},
			base: agreed, node: actions.Node{FileID: "srv1", Hash: peerNew, Size: 4},
			wantErr: errUploadTargetDiverged,
		},
		{
			// The op carries no id, the server's reply carries one. That is the ABSENCE of
			// identity evidence, not contradicting identity - read as a mismatch it funnelled
			// exactly the ordinary modifies above, one arm further down.
			name: "modify: id-less against an answering server id is not a node mismatch",
			op:   store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours, BaseLocalHash: agreed},
			base: "", node: actions.Node{FileID: "srv1", Size: 4},
			wantPass: true,
		},
		{
			// A bare reply is no occupancy for an id-less modify either; it degrades to identity
			// with the inertness logged, exactly as the id-bearing modify above does.
			name:     "modify: id-less, a bare reply asserts nothing and is not occupancy",
			op:       store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours, BaseLocalHash: agreed},
			base:     agreed,
			node:     actions.Node{},
			wantPass: true,
		},
		{
			// And the sentinel: a modify still corroborates before reading it as absence, whether
			// or not it carries an id - the push behind it overwrites either way.
			name:    "modify: id-less, one path-error sentinel does not prove the node gone",
			op:      store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours, BaseLocalHash: agreed},
			base:    agreed,
			nodeErr: actions.ErrPathError,
			wantErr: errUploadTargetAbsenceUnproved,
		},
		{
			// An op with no scan-time hash of its own (nothing to adopt by content) against an
			// occupant that reports none either: identity is the only evidence left, and for a
			// CREATE any identified occupant is a peer's file.
			name:    "create: an occupant reporting no hash at all is still occupancy",
			op:      store.Op{ViewID: 7, Path: "a.txt"},
			node:    actions.Node{FileID: "peer1", Size: 4},
			wantErr: errUploadTargetDiverged,
		},
		{
			name:    "create: path already occupied by a peer's node",
			op:      store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			node:    actions.Node{FileID: "peer1", Hash: peerNew, Size: 4},
			wantErr: errUploadTargetDiverged,
		},
		{
			name:     "create: the occupant is our own already-landed content",
			op:       store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			node:     actions.Node{FileID: "peer1", Hash: ours, Size: 4},
			wantPass: true,
		},
		{
			name:     "create: a bare reply asserts nothing and is not occupancy",
			op:       store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			node:     actions.Node{},
			wantPass: true,
		},
		{
			name:    "a directory holds the path",
			op:      store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			node:    actions.Node{FileID: "dir1", IsDir: true},
			wantErr: errServerTypeCollision,
		},
		{
			// is_dir is occupancy evidence in its OWN right: a query_node request carries a path
			// and nothing else, so the server cannot echo the flag back, and a directory reply is
			// exactly the shape that can carry no id, no hash, no size and no mtime (a directory
			// has no content hash at all). Read after the nodeAnswered exit, such a reply left the
			// guard silent and the push ran onto a directory - refused by the server as 24581 and
			// dead-lettered on the shortened name-rejection window instead of routed to the
			// file<->dir funnel this verdict exists to reach.
			name:    "a directory whose reply asserts nothing else still holds the path",
			op:      store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			node:    actions.Node{IsDir: true},
			wantErr: errServerTypeCollision,
		},
		{
			// The sentinel is the ordinary answer for a path that is free, and a create cannot
			// be made to wait absentNodeConfirmations drain passes for it.
			name:     "create: the path-error sentinel means the path is free",
			op:       store.Op{ViewID: 7, Path: "a.txt", LocalHash: ours},
			nodeErr:  actions.ErrPathError,
			wantPass: true,
		},
		{
			// A modify may not read one sentinel as "the node is gone": 28672 also reports a
			// server-side timeout, and the push behind it overwrites. Uncorroborated, it is
			// undecided - the op stays queued (uploadTargetAbsent).
			name:    "modify: one path-error sentinel does not prove the node gone",
			op:      store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: ours},
			base:    agreed,
			nodeErr: actions.ErrPathError,
			wantErr: errUploadTargetAbsenceUnproved,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{
				queryNode:    map[string]actions.Node{"a.txt": tc.node},
				queryNodeErr: tc.nodeErr,
			}
			e, _, _ := outboundEngine(t, c, "Photos", "root1")
			err := e.guardUploadTarget(context.Background(), c, tc.op, tc.base)
			switch {
			case tc.wantPass && err != nil:
				t.Fatalf("guardUploadTarget = %v, want the push allowed", err)
			case !tc.wantPass && !errors.Is(err, tc.wantErr):
				t.Fatalf("guardUploadTarget = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestUploadDivergenceBaseUsesServerHashOnly pins the base reader. It must NOT fall back to the
// record's LocalHash the way removeTargetFacts does: planUpload has just stamped LocalHash with
// the new, un-pushed edit, so comparing the server against it would read "diverged" for every
// ordinary modify and funnel every push into keep-both.
func TestUploadDivergenceBaseUsesServerHashOnly(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "", LocalHash: "the-unpushed-edit",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	got, err := e.uploadDivergenceBase(store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1"})
	if err != nil {
		t.Fatalf("uploadDivergenceBase: %v", err)
	}
	if got != "" {
		t.Errorf("base = %q, want empty (the un-pushed LocalHash must never stand in for the base)", got)
	}
	// A create reads nothing at all: it has no agreed base and its guard is occupancy.
	if got, err := e.uploadDivergenceBase(store.Op{ViewID: 7, Path: "a.txt"}); err != nil || got != "" {
		t.Errorf("create base = %q (err %v), want empty", got, err)
	}
	// But an ID-LESS MODIFY is not a create, and suppressing its base read is what made the guard
	// classify it as one. It reads the record like any other modify.
	if err := st.PutBaseline(7, "idless.txt", store.Baseline{
		FileType: "file", ServerHash: "agreed-idless", LocalHash: "the-unpushed-edit",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	idless := store.Op{ViewID: 7, Path: "idless.txt", BaseLocalHash: "last-agreed"}
	if got, err := e.uploadDivergenceBase(idless); err != nil || got != "agreed-idless" {
		t.Errorf("id-less modify base = %q (err %v), want the record's ServerHash", got, err)
	}
	// The ordinary case still reports the agreed server hash.
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "srv2", FileType: "file", ServerHash: "agreed", LocalHash: "edit",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	if got, err := e.uploadDivergenceBase(store.Op{ViewID: 7, Path: "b.txt", FileID: "srv2"}); err != nil || got != "agreed" {
		t.Errorf("base = %q (err %v), want \"agreed\"", got, err)
	}
}

// TestUploadDivergenceBaseFallsBackToOpCapture pins the fallback that keeps H1 from going inert.
// applyUploadOut writes the upload reply's hash into ServerHash verbatim, so a reply shape that
// omits it BLANKS the field on every successful push - and with an empty base guardUploadTarget
// degrades to node IDENTITY, which a peer's overwrite of the same node always satisfies. That is
// not a weaker form of the guard, it is the H1 silent loss itself. planUpload therefore captures
// the SYNCED record's LocalHash on the op at reconcile time (before its own apply stamps the new
// edit over it), and that capture stands in.
//
// It is the op's field, never the record's LocalHash, which by apply time is the un-pushed edit -
// the distinction TestUploadDivergenceBaseUsesServerHashOnly above pins from the other side.
func TestUploadDivergenceBaseFallsBackToOpCapture(t *testing.T) {
	c := &pushClient{}
	e, st, _ := outboundEngine(t, c, "Photos", "root1")
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "", LocalHash: "the-unpushed-edit",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", BaseLocalHash: "last-agreed"}
	if got, err := e.uploadDivergenceBase(op); err != nil || got != "last-agreed" {
		t.Errorf("base = %q (err %v), want \"last-agreed\"", got, err)
	}
	// A present ServerHash still wins: it is the server's own statement about the node.
	if err := st.PutBaseline(7, "b.txt", store.Baseline{
		FileID: "srv2", FileType: "file", ServerHash: "agreed", LocalHash: "edit",
		State: store.StatePending,
	}); err != nil {
		t.Fatalf("seed baseline: %v", err)
	}
	op2 := store.Op{ViewID: 7, Path: "b.txt", FileID: "srv2", BaseLocalHash: "stale-capture"}
	if got, err := e.uploadDivergenceBase(op2); err != nil || got != "agreed" {
		t.Errorf("base = %q (err %v), want \"agreed\" (the record's ServerHash is primary)", got, err)
	}
	// The capture is independent of the record precisely so it survives the record's absence -
	// the crash window between localReconcile's Enqueue and its baseline flip.
	op3 := store.Op{ViewID: 7, Path: "gone.txt", FileID: "srv3", BaseLocalHash: "last-agreed"}
	if got, err := e.uploadDivergenceBase(op3); err != nil || got != "last-agreed" {
		t.Errorf("absent-record base = %q (err %v), want \"last-agreed\"", got, err)
	}
}

// TestPlanUploadCapturesSyncedBaseAsDivergenceBase pins the producing side: only a modify off a
// StateSynced record may supply the capture. A fresh create has no agreed base at all, and the
// guard's question there is occupancy rather than content.
func TestPlanUploadCapturesSyncedBaseAsDivergenceBase(t *testing.T) {
	c := &pushClient{}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	en := scanEntry{relPath: "a.txt", md4: "new-edit", size: 4, mtime: 100}

	modify := e.planUpload(7, en, "srv1", "a.txt", "last-agreed", true)
	if modify.op.BaseLocalHash != "last-agreed" {
		t.Errorf("modify BaseLocalHash = %q, want %q", modify.op.BaseLocalHash, "last-agreed")
	}
	if modify.op.LocalHash != "new-edit" {
		t.Errorf("modify LocalHash = %q, want the edit being pushed", modify.op.LocalHash)
	}
	create := e.planUpload(7, en, "", "a.txt", "", false)
	if create.op.BaseLocalHash != "" {
		t.Errorf("create BaseLocalHash = %q, want empty", create.op.BaseLocalHash)
	}
}

// TestLocalReconcileCapturesTheSyncedBaseOnAnOutboundUpload pins the PRODUCER of the H1 guard's
// fallback divergence base, which every other case in this file supplies by hand.
//
// The capture is what keeps the guard from going inert: applyUploadOut assigns the upload reply's
// hash to ServerHash verbatim, so a reply shape that omits it BLANKS the field on every successful
// push. With no ServerHash and no Op.BaseLocalHash, uploadDivergenceBase returns nothing and the
// guard degrades to node IDENTITY - which a peer's overwrite of the same node always satisfies,
// i.e. finding H1 itself. So the reconcile-time capture is asserted here against a record whose
// ServerHash is exactly that: empty.
func TestLocalReconcileCapturesTheSyncedBaseOnAnOutboundUpload(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	agreed := md4.ContentHash([]byte("agreed"))
	writeFile(t, viewDir, "a.txt", []byte("agreed"))
	// A record left by a push whose reply carried no hash: synced, with a LocalHash and NO
	// ServerHash. This is the shape the fallback exists for.
	if err := e.store.PutBaseline(viewID, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", LocalHash: agreed, LocalSize: 6, State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	writeFile(t, viewDir, "a.txt", []byte("the local edit"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload {
		t.Fatalf("ops = %+v, want one OpUpload", ops)
	}
	if ops[0].BaseLocalHash != agreed {
		t.Fatalf("BaseLocalHash = %q, want the SYNCED record's hash %q; without it the guard "+
			"degrades to node identity and a peer's overwrite passes as agreement",
			ops[0].BaseLocalHash, agreed)
	}
	// And the record's own LocalHash is now the un-pushed edit, which is exactly why the guard may
	// not fall back there.
	b, err := e.store.GetBaseline(viewID, "a.txt")
	if err != nil {
		t.Fatalf("GetBaseline: %v", err)
	}
	if b.LocalHash == agreed {
		t.Error("the pending record still holds the agreed hash; the capture would be redundant")
	}
	if got, derr := e.uploadDivergenceBase(ops[0]); derr != nil || got != agreed {
		t.Errorf("uploadDivergenceBase = %q (err %v), want the captured base %q", got, derr, agreed)
	}
}

// TestUploadDivergenceBaseFallsBackOnACorruptRecord pins the arm that deliberately differs from
// its sibling precondition: removeTargetFacts REFUSES on an undecodable record (the kind it needs
// is exactly what corrupt bytes cannot supply), while the upload guard degrades to the op's own
// reconcile-time capture - the base is carried on the op precisely so the record's health is not
// a precondition of pushing safely.
func TestUploadDivergenceBaseFallsBackOnACorruptRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := st.PutBaseline(7, "a.txt", store.Baseline{
		FileID: "srv1", FileType: "file", ServerHash: "agreed", LocalHash: "agreed",
		State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	corruptBaselineRecord(t, dbPath, 7, "a.txt")
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("store.Open after corruption: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	e := New(st2, &concTransport{client: &pushClient{}}, t.TempDir(), "sess", nil)

	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", BaseLocalHash: "last-agreed"}
	got, berr := e.uploadDivergenceBase(op)
	if berr != nil {
		t.Fatalf("uploadDivergenceBase = %v, want the op's capture rather than a failure", berr)
	}
	if got != "last-agreed" {
		t.Errorf("base = %q, want the op's reconcile-time capture %q", got, "last-agreed")
	}
}

// TestUploadGuardModifyLogsInertnessOnAnAnswerlessReply pins that the H1 guard is never SILENTLY
// vacuous. A modify whose probe answers nothing (none of nodeAnswered's four facts) cannot be
// decided either way: the push proceeds, exactly as it does when the hashes are merely missing.
// But the two shapes used to differ in observability - the missing-hash arm logs the degradation
// to node identity, while the answerless reply took an early return and logged nothing at all.
// Against a DSM whose query_node reply carries none of those facts, that is the whole H1
// precondition silently doing nothing, which is the one state an operator must be able to see.
//
// The create keeps the unlogged exit: for a create an answerless reply is not a degraded answer,
// it is the ORDINARY answer for a free path.
func TestUploadGuardModifyLogsInertnessOnAnAnswerlessReply(t *testing.T) {
	for _, tc := range []struct {
		name    string
		op      store.Op
		wantLog bool
	}{
		{
			name:    "modify",
			op:      store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: "ours"},
			wantLog: true,
		},
		{
			name:    "create",
			op:      store.Op{ViewID: 7, Path: "a.txt", LocalHash: "ours"},
			wantLog: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &pushClient{queryNode: map[string]actions.Node{"a.txt": {}}}
			st := newTestStore(t)
			if err := st.PutView(7, store.View{Name: "Photos", NodeID: "root1"}); err != nil {
				t.Fatalf("PutView: %v", err)
			}
			e, logs := captureEngine(t, st, &concTransport{client: c})

			// Both shapes must still ALLOW the push: an answerless reply proves no occupancy, and
			// funnelling it into keep-both would manufacture a _Conflict copy out of nothing.
			if err := e.guardUploadTarget(context.Background(), c, tc.op, ""); err != nil {
				t.Fatalf("guardUploadTarget = %v, want the push allowed", err)
			}
			// countWarns, not countRecords: the LEVEL is load-bearing. config.Defaults ships
			// LogLevel "info", so a Debug line never reaches the rotating log on a default
			// install and the inertness this guard promises to surface would be invisible on
			// exactly the DSM that makes it total.
			got := logs.countWarns("degraded to node identity")
			if tc.wantLog && got == 0 {
				t.Error("no degradation Warn: the guard went inert without saying so")
			}
			if !tc.wantLog && got != 0 {
				t.Errorf("degradation logged %d time(s) for a create; an empty reply is the ordinary answer for a free path", got)
			}
		})
	}
}

// TestGuardUploadTargetAbsenceResetByAFailedProbe pins the reset for the answer the sibling above
// never drives: a probe that fails on TRANSPORT is not an observation of anything, so it breaks
// the run exactly as an answering node does. Counting it as a mere gap would corroborate an
// absence from one real sentinel plus one non-observation, after which the MODIFY pushes under
// compare_mtime and destroys the peer's version - finding H1 through the corroboration itself.
func TestGuardUploadTargetAbsenceResetByAFailedProbe(t *testing.T) {
	const agreed = "agreed-base-hash"
	op := store.Op{ViewID: 7, Path: "a.txt", FileID: "srv1", LocalHash: "ours"}

	c := &pushClient{queryNodeErr: actions.ErrPathError}
	e, _, _ := outboundEngine(t, c, "Photos", "root1")
	now := time.Unix(1_700_000_000, 0)
	e.now = func() time.Time { return now }

	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("one path-error probe allowed the push; the absence is uncorroborated")
	}

	// A transport failure: undecided, so the push is refused - and the run is broken with it.
	c.queryNodeErr = actions.ErrConnBroken
	now = now.Add(absenceStreakMinAge)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("a failed probe allowed the push; an undecided probe never authorises an overwrite")
	}

	// The next sentinel is a FIRST probe again, so it cannot complete the verdict on its own.
	c.queryNodeErr = actions.ErrPathError
	now = now.Add(absenceStreakMinAge)
	e.beginDrainPass()
	if err := e.guardUploadTarget(context.Background(), c, op, agreed); err == nil {
		t.Fatal("the sentinel after a failed probe completed a verdict; a non-observation is not evidence")
	}
}

// TestFactlessSyncedRecordModifyIsNotGuardedAsACreate pins the CLASS discriminator against the one
// shape an absence-of-facts reading cannot see.
//
// uploadIsCreate originally asked "no file id AND no captured base" - an absence of evidence. But a
// StateSynced record can legitimately carry neither, and two live arms produce exactly that against
// a DSM whose replies omit file_id: applyRename records bare facts (store.Baseline{FileID:
// op.FileID}) for a node whose id-less move left nothing richer to carry, and
// revertRelocatedOutbound clears LocalHash while restoring StateSynced for a push it re-emits. The
// next ordinary edit of such a file then read as a CREATE, took the OCCUPANCY guard, and was
// funnelled into a spurious _Conflict copy - with the server's older bytes downloaded back over the
// canonical path - which is precisely the churn the create/modify split exists to prevent.
//
// So the answer is positive: BaseWasSynced, set only by the modify arm that holds the synced record.
func TestFactlessSyncedRecordModifyIsNotGuardedAsACreate(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	writeFile(t, viewDir, "a.txt", []byte("agreed"))
	// The bare record applyRename leaves for an id-less moved node: synced, no FileID, no
	// LocalHash, no ServerHash.
	if err := e.store.PutBaseline(viewID, "a.txt", store.Baseline{
		FileType: "file", State: store.StateSynced,
	}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	writeFile(t, viewDir, "a.txt", []byte("the local edit"))

	ops, err := e.localReconcile(context.Background(), viewID)
	if err != nil {
		t.Fatalf("localReconcile: %v", err)
	}
	if len(ops) != 1 || ops[0].Type != store.OpUpload {
		t.Fatalf("ops = %+v, want one OpUpload", ops)
	}
	op := ops[0]
	if op.FileID != "" || op.BaseLocalHash != "" {
		t.Fatalf("op = %+v, want the factless shape this test is about", op)
	}
	if !op.BaseWasSynced {
		t.Error("BaseWasSynced is false on a modify off a StateSynced record; the guard's class " +
			"discrimination has nothing positive left to read")
	}
	if uploadIsCreate(op) {
		t.Fatal("a modify off a synced record was classified as a CREATE; its own server node " +
			"occupying its own path then funnels an ordinary edit into a spurious _Conflict copy")
	}

	// End to end through the guard: the server still holds the node's older bytes, which is the
	// occupancy a create would have refused on. As a modify with no comparable hashes it degrades
	// to node identity and the push PROCEEDS.
	c := &pushClient{queryNode: map[string]actions.Node{
		"a.txt": {FileID: "srv1", Hash: md4.ContentHash([]byte("agreed"))},
	}}
	base, err := e.uploadDivergenceBase(op)
	if err != nil {
		t.Fatalf("uploadDivergenceBase: %v", err)
	}
	if gerr := e.guardUploadTarget(context.Background(), c, op, base); gerr != nil {
		t.Errorf("guardUploadTarget = %v, want nil: an ordinary edit of a node the client already "+
			"tracks must push, not funnel into keep-both", gerr)
	}
}
