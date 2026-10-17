package store

import (
	"errors"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func sampleOp(path string) Op {
	return Op{
		Type:       OpUpload,
		ViewID:     1,
		Path:       path,
		FileID:     "fid-" + path,
		LocalHash:  "aabbcc",
		LocalSize:  1024,
		LocalMTime: 1000,
		UnixPerm:   0o644,
	}
}

func TestPendingOpsNBoundsTheRead(t *testing.T) {
	s, _ := openTemp(t)

	paths := []string{"a", "b", "c", "d", "e"}
	for _, p := range paths {
		if _, err := s.Enqueue(sampleOp(p)); err != nil {
			t.Fatalf("Enqueue %q: %v", p, err)
		}
	}

	// A positive limit returns at most that many ops, in FIFO order.
	got, err := s.PendingOpsN(2)
	if err != nil {
		t.Fatalf("PendingOpsN(2): %v", err)
	}
	if len(got) != 2 || got[0].Path != "a" || got[1].Path != "b" {
		t.Fatalf("PendingOpsN(2) = %+v, want the first two ops [a b]", got)
	}

	// A limit at or above the queue length returns everything.
	if all, err := s.PendingOpsN(100); err != nil || len(all) != len(paths) {
		t.Fatalf("PendingOpsN(100) len = %d, err %v, want %d", len(all), err, len(paths))
	}

	// A non-positive limit returns the whole queue, matching PendingOps.
	if all, err := s.PendingOpsN(0); err != nil || len(all) != len(paths) {
		t.Fatalf("PendingOpsN(0) len = %d, err %v, want %d", len(all), err, len(paths))
	}
	if none, err := s.PendingOpsN(-1); err != nil || len(none) != len(paths) {
		t.Fatalf("PendingOpsN(-1) len = %d, err %v, want %d", len(none), err, len(paths))
	}
}

// TestPendingOpsNFromPagesPastSeq pins the paging reader: afterSeq
// is an exclusive lower bound (mirroring ScanPendingFrom) and the limit still bounds
// the window, so the executor can slide its bounded drain window past ops an earlier
// window left queued instead of re-reading a saturated head forever.
func TestPendingOpsNFromPagesPastSeq(t *testing.T) {
	s, _ := openTemp(t)

	paths := []string{"a", "b", "c", "d", "e"}
	seqs := make([]uint64, len(paths))
	for i, p := range paths {
		seq, err := s.Enqueue(sampleOp(p))
		if err != nil {
			t.Fatalf("Enqueue %q: %v", p, err)
		}
		seqs[i] = seq
	}

	// afterSeq == 0 reads from the head, like PendingOpsN.
	head, err := s.PendingOpsNFrom(0, 2)
	if err != nil {
		t.Fatalf("PendingOpsNFrom(0, 2): %v", err)
	}
	if len(head) != 2 || head[0].Path != "a" || head[1].Path != "b" {
		t.Fatalf("PendingOpsNFrom(0, 2) = %+v, want [a b]", head)
	}

	// afterSeq is exclusive: paging from b's seq resumes at c, still limit-bounded.
	window, err := s.PendingOpsNFrom(seqs[1], 2)
	if err != nil {
		t.Fatalf("PendingOpsNFrom(seq b, 2): %v", err)
	}
	if len(window) != 2 || window[0].Path != "c" || window[1].Path != "d" {
		t.Fatalf("PendingOpsNFrom(seq b, 2) = %+v, want [c d]", window)
	}

	// A non-positive limit returns everything past the cursor.
	rest, err := s.PendingOpsNFrom(seqs[2], 0)
	if err != nil {
		t.Fatalf("PendingOpsNFrom(seq c, 0): %v", err)
	}
	if len(rest) != 2 || rest[0].Path != "d" || rest[1].Path != "e" {
		t.Fatalf("PendingOpsNFrom(seq c, 0) = %+v, want [d e]", rest)
	}

	// A cursor at (or past) the tail yields an empty window.
	if none, err := s.PendingOpsNFrom(seqs[4], 10); err != nil || len(none) != 0 {
		t.Fatalf("PendingOpsNFrom(tail) = %+v (err %v), want empty", none, err)
	}
}

func TestScanPendingStreamsInFIFOOrderAndPropagatesErrors(t *testing.T) {
	s, _ := openTemp(t)

	paths := []string{"a", "b", "c", "d"}
	for _, p := range paths {
		if _, err := s.Enqueue(sampleOp(p)); err != nil {
			t.Fatalf("Enqueue %q: %v", p, err)
		}
	}

	// A full scan visits every op once, in ascending-seq (FIFO) order.
	var seen []string
	if err := s.ScanPending(func(qo QueuedOp) error {
		seen = append(seen, qo.Path)
		return nil
	}); err != nil {
		t.Fatalf("ScanPending: %v", err)
	}
	if len(seen) != len(paths) {
		t.Fatalf("ScanPending visited %v, want %v", seen, paths)
	}
	for i, p := range paths {
		if seen[i] != p {
			t.Fatalf("ScanPending order = %v, want %v", seen, paths)
		}
	}

	// A non-nil error from the callback stops the scan and propagates out.
	sentinel := errors.New("stop")
	var visited int
	err := s.ScanPending(func(qo QueuedOp) error {
		visited++
		if qo.Path == "b" {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("ScanPending error = %v, want sentinel", err)
	}
	if visited != 2 {
		t.Fatalf("ScanPending visited %d ops before stopping, want 2", visited)
	}

	// Collecting seqs during the scan and Acking them afterwards is the supported
	// drop pattern (the cursor is live during the scan, so no mutation happens inside).
	var dropSeqs []uint64
	if err := s.ScanPending(func(qo QueuedOp) error {
		if qo.Path == "a" || qo.Path == "c" {
			dropSeqs = append(dropSeqs, qo.Seq)
		}
		return nil
	}); err != nil {
		t.Fatalf("ScanPending collect: %v", err)
	}
	for _, seq := range dropSeqs {
		if err := s.Ack(seq); err != nil {
			t.Fatalf("Ack %d: %v", seq, err)
		}
	}
	remaining, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after drop: %v", err)
	}
	if len(remaining) != 2 || remaining[0].Path != "b" || remaining[1].Path != "d" {
		t.Fatalf("after dropping a,c remaining = %+v, want [b d]", remaining)
	}
}

func TestScanPendingFromResumesPastSeq(t *testing.T) {
	s, _ := openTemp(t)

	paths := []string{"a", "b", "c", "d", "e"}
	seqs := make([]uint64, len(paths))
	for i, p := range paths {
		seq, err := s.Enqueue(sampleOp(p))
		if err != nil {
			t.Fatalf("Enqueue %q: %v", p, err)
		}
		seqs[i] = seq
	}

	// afterSeq == 0 scans the whole queue (matching ScanPending).
	var all []string
	if err := s.ScanPendingFrom(0, func(qo QueuedOp) error {
		all = append(all, qo.Path)
		return nil
	}); err != nil {
		t.Fatalf("ScanPendingFrom(0): %v", err)
	}
	if len(all) != len(paths) || all[0] != "a" || all[4] != "e" {
		t.Fatalf("ScanPendingFrom(0) = %v, want %v", all, paths)
	}

	// afterSeq is an exclusive lower bound: starting at b's seq resumes at c.
	var rest []string
	if err := s.ScanPendingFrom(seqs[1], func(qo QueuedOp) error {
		rest = append(rest, qo.Path)
		return nil
	}); err != nil {
		t.Fatalf("ScanPendingFrom(seq b): %v", err)
	}
	want := []string{"c", "d", "e"}
	if len(rest) != len(want) {
		t.Fatalf("ScanPendingFrom past b = %v, want %v", rest, want)
	}
	for i, p := range want {
		if rest[i] != p {
			t.Fatalf("ScanPendingFrom past b = %v, want %v", rest, want)
		}
	}

	// Resuming past an acked (gone) seq still lands on the next surviving op.
	if err := s.Ack(seqs[2]); err != nil { // drop c
		t.Fatalf("Ack c: %v", err)
	}
	var afterC []string
	if err := s.ScanPendingFrom(seqs[2], func(qo QueuedOp) error {
		afterC = append(afterC, qo.Path)
		return nil
	}); err != nil {
		t.Fatalf("ScanPendingFrom(seq c): %v", err)
	}
	if len(afterC) != 2 || afterC[0] != "d" || afterC[1] != "e" {
		t.Fatalf("ScanPendingFrom past acked c = %v, want [d e]", afterC)
	}
}

func TestEnqueueFIFOOrder(t *testing.T) {
	s, _ := openTemp(t)

	if n, err := s.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen empty: got %d, err %v", n, err)
	}
	if ops, err := s.PendingOps(); err != nil || len(ops) != 0 {
		t.Fatalf("PendingOps empty: got %d ops, err %v", len(ops), err)
	}

	paths := []string{"a", "b", "c"}
	var seqs []uint64
	for _, p := range paths {
		seq, err := s.Enqueue(sampleOp(p))
		if err != nil {
			t.Fatalf("Enqueue %q: %v", p, err)
		}
		seqs = append(seqs, seq)
	}
	// Sequences must be strictly increasing.
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Errorf("seq not monotonic: %v", seqs)
		}
	}

	if n, err := s.PendingLen(); err != nil || n != len(paths) {
		t.Fatalf("PendingLen: got %d, err %v, want %d", n, err, len(paths))
	}

	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != len(paths) {
		t.Fatalf("PendingOps len = %d, want %d", len(ops), len(paths))
	}
	for i, p := range paths {
		if ops[i].Path != p {
			t.Errorf("PendingOps[%d].Path = %q, want %q", i, ops[i].Path, p)
		}
		if ops[i].Seq != seqs[i] {
			t.Errorf("PendingOps[%d].Seq = %d, want %d", i, ops[i].Seq, seqs[i])
		}
	}

	// The head is the oldest op; reading it does not remove anything.
	if ops[0].Path != "a" || ops[0].Seq != seqs[0] {
		t.Errorf("head = %+v, want path a seq %d", ops[0], seqs[0])
	}
	if n, _ := s.PendingLen(); n != len(paths) {
		t.Errorf("reading the queue removed an op: PendingLen = %d", n)
	}

	// Acking by seq drains in FIFO order.
	for i, p := range paths {
		head, err := s.PendingOpsN(1)
		if err != nil {
			t.Fatalf("PendingOpsN(1): %v", err)
		}
		if len(head) != 1 || head[0].Path != p {
			t.Errorf("head = %+v, want %q", head, p)
		}
		if err := s.Ack(seqs[i]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
	if n, _ := s.PendingLen(); n != 0 {
		t.Errorf("PendingLen after drain = %d, want 0", n)
	}
}

func TestEnqueueRoundTripFields(t *testing.T) {
	s, _ := openTemp(t)

	// Every JSON-serialized field is set to a non-zero value so the struct-equality
	// check below pins the round-trip of each one (a wrong/typo'd json tag would
	// otherwise drop a field silently). The values are a serialization contract, not
	// a semantically coherent op.
	want := Op{
		Type:               OpRename,
		Direction:          DirOutbound,
		ViewID:             42,
		Path:               "old/p",
		NewPath:            "new/p",
		IsDir:              true,
		FileID:             "fid-x",
		ServerSyncID:       99,
		LocalHash:          "ddeeff",
		LocalSize:          4096,
		LocalMTime:         2000,
		UnixPerm:           0o600,
		BaseLocalHash:      "aabbcc",
		DestBaseLocalHash:  "112233",
		DestUntracked:      true,
		BaseWasSynced:      true,
		BaseIsDir:          true,
		MovedBaseSynced:    true,
		MovedBaseLocalHash: "445566",
	}
	seq, err := s.Enqueue(want)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("PendingOps len = %d, want 1", len(ops))
	}
	got := ops[0]
	if got.Seq != seq {
		t.Errorf("Seq = %d, want %d", got.Seq, seq)
	}
	if got.Op != want {
		t.Errorf("Op = %+v, want %+v", got.Op, want)
	}
}

func TestEnqueueDirectionRoundTrip(t *testing.T) {
	s, _ := openTemp(t)

	want := Op{
		Type:      OpDownload,
		Direction: DirInbound,
		ViewID:    7,
		Path:      "dir/file.txt",
		FileID:    "fid-y",
		LocalHash: "001122",
		LocalSize: 10,
	}
	seq, err := s.Enqueue(want)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Reading the queue must preserve Direction across the round-trip.
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Direction != DirInbound {
		t.Errorf("PendingOps = %+v, want one DirInbound op", ops)
	}
	got := ops[0]
	if got.Seq != seq || got.Op != want {
		t.Errorf("op = %+v, want seq %d op %+v", got, seq, want)
	}
}

func TestEnqueueEmptyDirectionDecodesAsUnset(t *testing.T) {
	s, _ := openTemp(t)

	// An op written without a Direction (older records, or the field unset) must
	// read back as the zero string, not error.
	if _, err := s.Enqueue(sampleOp("legacy")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Direction != "" {
		t.Errorf("Direction = %q, want empty", ops[0].Direction)
	}
}

func TestAckRemovesBySeq(t *testing.T) {
	s, _ := openTemp(t)

	s1, _ := s.Enqueue(sampleOp("a"))
	s2, _ := s.Enqueue(sampleOp("b"))

	// Ack the second op out of order; the first must remain.
	if err := s.Ack(s2); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Seq != s1 {
		t.Fatalf("after Ack = %+v, want only seq %d", ops, s1)
	}
	// Acking an absent seq is a no-op.
	if err := s.Ack(s2); err != nil {
		t.Errorf("Ack absent: %v", err)
	}
	if err := s.Ack(s1); err != nil {
		t.Fatalf("Ack first: %v", err)
	}
	if n, _ := s.PendingLen(); n != 0 {
		t.Errorf("PendingLen = %d, want 0", n)
	}
}

func TestQueueSurvivesReopen(t *testing.T) {
	s, path := openTemp(t)

	a, _ := s.Enqueue(sampleOp("alpha"))
	b, _ := s.Enqueue(sampleOp("beta"))
	// Drain one so the seq counter is past it; the remaining op must persist.
	if err := s.Ack(a); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	ops, err := s2.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps after reopen: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "beta" || ops[0].Seq != b {
		t.Fatalf("after reopen = %+v, want single beta seq %d", ops, b)
	}

	// NextSequence must still advance past the pre-restart maximum.
	next, err := s2.Enqueue(sampleOp("gamma"))
	if err != nil {
		t.Fatalf("Enqueue after reopen: %v", err)
	}
	if next <= b {
		t.Errorf("seq after reopen = %d, want > %d", next, b)
	}
}

// corruptQueueValue overwrites the queue record at seq with an undecodable value,
// simulating latent file corruption (bbolt checksums only its meta pages).
func corruptQueueValue(t *testing.T, s *Store, seq uint64) {
	t.Helper()
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketQueue).Put(encodeUint64(seq), []byte("{corrupt"))
	})
	if err != nil {
		t.Fatalf("corrupt queue value %d: %v", seq, err)
	}
}

// A corrupt queue value must not poison the whole queue: before
// the fix every reader returned an error on the first undecodable record, so
// drainQueue, the outbound scan's queuedPaths and reconcile's supersession scans all
// failed forever - even the healthy ops before the corrupt one were unreachable, and
// nothing exposed the seq to Ack it away short of Reset/re-pair.
func TestPendingOpsNSkipsAndQuarantinesCorruptValue(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	b, _ := s.Enqueue(sampleOp("b"))
	c, _ := s.Enqueue(sampleOp("c"))
	corruptQueueValue(t, s, b)

	got, err := s.PendingOpsN(0)
	if err != nil {
		t.Fatalf("PendingOpsN over corrupt record: %v", err)
	}
	if len(got) != 2 || got[0].Seq != a || got[0].Path != "a" || got[1].Seq != c || got[1].Path != "c" {
		t.Fatalf("PendingOpsN = %+v, want healthy ops [a c]", got)
	}

	// The corrupt record left the queue and became a durable dead-letter trace
	// (surfaced as the `status` Dropped count).
	n, err := s.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 2 {
		t.Fatalf("PendingLen after quarantine = %d, want 2", n)
	}
	dls, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead-letters = %+v, want exactly one", dls)
	}
	if !strings.Contains(dls[0].Reason, "corrupt queue record") || !strings.Contains(dls[0].Reason, "seq 2") {
		t.Errorf("dead-letter reason = %q, want corrupt-queue-record reason carrying the raw seq", dls[0].Reason)
	}
	if dls[0].RecordedAt == 0 {
		t.Errorf("dead-letter RecordedAt = 0, want a timestamp")
	}

	// A second read must not double-count Dropped: the record is already gone.
	if _, err := s.PendingOpsN(0); err != nil {
		t.Fatalf("second PendingOpsN: %v", err)
	}
	if n, _ := s.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen after second read = %d, want still 1", n)
	}
}

// A corrupt record must not count toward PendingOpsN's limit, or a batch-sized run
// of corruption would starve the healthy ops behind it out of every drain batch.
func TestPendingOpsNCorruptValueDoesNotConsumeLimit(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	corruptQueueValue(t, s, a)
	_, _ = s.Enqueue(sampleOp("b"))
	_, _ = s.Enqueue(sampleOp("c"))

	got, err := s.PendingOpsN(2)
	if err != nil {
		t.Fatalf("PendingOpsN(2): %v", err)
	}
	if len(got) != 2 || got[0].Path != "b" || got[1].Path != "c" {
		t.Fatalf("PendingOpsN(2) = %+v, want [b c]", got)
	}
}

func TestScanPendingSkipsAndQuarantinesCorruptValue(t *testing.T) {
	s, _ := openTemp(t)

	_, _ = s.Enqueue(sampleOp("a"))
	b, _ := s.Enqueue(sampleOp("b"))
	_, _ = s.Enqueue(sampleOp("c"))
	corruptQueueValue(t, s, b)

	var paths []string
	err := s.ScanPending(func(qo QueuedOp) error {
		paths = append(paths, qo.Path)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPending over corrupt record: %v", err)
	}
	if len(paths) != 2 || paths[0] != "a" || paths[1] != "c" {
		t.Fatalf("ScanPending visited %v, want [a c]", paths)
	}
	if n, _ := s.PendingLen(); n != 2 {
		t.Fatalf("PendingLen after quarantine = %d, want 2", n)
	}
	if n, _ := s.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1", n)
	}
}

func TestScanPendingFromSkipsCorruptValueWhilePaging(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	b, _ := s.Enqueue(sampleOp("b"))
	c, _ := s.Enqueue(sampleOp("c"))
	corruptQueueValue(t, s, b)

	var seqs []uint64
	err := s.ScanPendingFrom(a, func(qo QueuedOp) error {
		seqs = append(seqs, qo.Seq)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPendingFrom over corrupt record: %v", err)
	}
	if len(seqs) != 1 || seqs[0] != c {
		t.Fatalf("ScanPendingFrom(%d) visited %v, want [%d]", a, seqs, c)
	}
	if n, _ := s.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1", n)
	}
}

// An fn error must still stop the scan and propagate - skip-and-quarantine applies
// only to undecodable records, not to the caller's own control flow.
func TestScanPendingCorruptValueKeepsFnErrorPropagation(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	corruptQueueValue(t, s, a)
	_, _ = s.Enqueue(sampleOp("b"))

	sentinel := errors.New("stop")
	err := s.ScanPending(func(QueuedOp) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("ScanPending fn error = %v, want sentinel", err)
	}
	// The corrupt record seen before the stop is still quarantined.
	if n, _ := s.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1", n)
	}
}

// A malformed (short) key used to panic decodeUint64 (binary.BigEndian.Uint64 on
// len<8), crashing the daemon out of a routine queue read; it is now quarantined
// like a corrupt value.
func TestQueueReadersSkipAndQuarantineCorruptShortKey(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketQueue).Put([]byte{0x01, 0x02}, []byte("junk"))
	})
	if err != nil {
		t.Fatalf("insert short key: %v", err)
	}

	got, err := s.PendingOpsN(0)
	if err != nil {
		t.Fatalf("PendingOpsN over short key: %v", err)
	}
	if len(got) != 1 || got[0].Seq != a {
		t.Fatalf("PendingOpsN = %+v, want the single healthy op", got)
	}
	if n, _ := s.PendingLen(); n != 1 {
		t.Fatalf("PendingLen after quarantine = %d, want 1", n)
	}
	dls, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(dls) != 1 || !strings.Contains(dls[0].Reason, "corrupt queue key") {
		t.Fatalf("dead-letters = %+v, want one corrupt-queue-key reason", dls)
	}
}

// TestQuarantinedOpsCountsEachDropExactlyOnce pins the counter the engine's repair sweep
// is armed by. A quarantine drop is the one op removal that cannot
// resolve the baseline reconcile pre-wrote for it - the record's view and path are exactly
// what failed to decode - so the engine treats an advance of this counter as its cue to
// re-derive the owners of pending baselines nothing covers. It must count a drop that
// actually committed, and count it once: a replayed read (the record is already gone) must
// not re-arm the sweep, or every drain would re-sweep forever after a single bit-rot.
func TestQuarantinedOpsCountsEachDropExactlyOnce(t *testing.T) {
	s, _ := openTemp(t)

	if n := s.QuarantinedOps(); n != 0 {
		t.Fatalf("QuarantinedOps on a fresh store = %d, want 0", n)
	}
	_, _ = s.Enqueue(sampleOp("a"))
	b, _ := s.Enqueue(sampleOp("b"))
	corruptQueueValue(t, s, b)

	if _, err := s.PendingOpsN(0); err != nil {
		t.Fatalf("PendingOpsN: %v", err)
	}
	if n := s.QuarantinedOps(); n != 1 {
		t.Fatalf("QuarantinedOps after the drop = %d, want 1", n)
	}
	// A healthy read changes nothing, and neither does a second read of the same
	// already-quarantined record.
	if _, err := s.PendingOpsN(0); err != nil {
		t.Fatalf("second PendingOpsN: %v", err)
	}
	if n := s.QuarantinedOps(); n != 1 {
		t.Fatalf("QuarantinedOps after a replayed read = %d, want still 1", n)
	}

	// A second, distinct corruption advances it again: the engine must sweep once more.
	c, _ := s.Enqueue(sampleOp("c"))
	corruptQueueValue(t, s, c)
	if err := s.ScanPending(func(QueuedOp) error { return nil }); err != nil {
		t.Fatalf("ScanPending: %v", err)
	}
	if n := s.QuarantinedOps(); n != 2 {
		t.Fatalf("QuarantinedOps after a second drop = %d, want 2", n)
	}
}

// The ScanPendingFrom Seek path (afterSeq > 0) must tolerate a malformed key too:
// the exact-match comparison decodes the landed-on key.
func TestScanPendingFromSeekTolerateShortKey(t *testing.T) {
	s, _ := openTemp(t)

	a, _ := s.Enqueue(sampleOp("a"))
	b, _ := s.Enqueue(sampleOp("b"))
	// A 2-byte key sorts after the 8-byte zero-prefixed seq keys, so the Seek
	// cursor walk crosses it.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketQueue).Put([]byte{0x01, 0x02}, []byte("junk"))
	})
	if err != nil {
		t.Fatalf("insert short key: %v", err)
	}

	var seqs []uint64
	err = s.ScanPendingFrom(a, func(qo QueuedOp) error {
		seqs = append(seqs, qo.Seq)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanPendingFrom over short key: %v", err)
	}
	if len(seqs) != 1 || seqs[0] != b {
		t.Fatalf("ScanPendingFrom(%d) visited %v, want [%d]", a, seqs, b)
	}
	if n, _ := s.DeadLetterLen(); n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1", n)
	}
}

// TestHasQueuedOps covers the second half of "is there work tied to the current
// layout?". The enqueue-first doctrine writes a queue record BEFORE its pending
// baseline, so a crash in that window leaves a queue with no baselines behind it
// - and a relayout would then recompute those ops' keys under the new layout.
func TestHasQueuedOps(t *testing.T) {
	s, _ := openTemp(t)

	got, err := s.HasQueuedOps()
	if err != nil {
		t.Fatalf("HasQueuedOps: %v", err)
	}
	if got {
		t.Error("a fresh store reports queued ops")
	}

	if _, err := s.Enqueue(Op{Type: OpUpload, Direction: DirOutbound, ViewID: 7, Path: "a.txt"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	got, err = s.HasQueuedOps()
	if err != nil {
		t.Fatalf("HasQueuedOps: %v", err)
	}
	if !got {
		t.Error("a store holding a queued op reports none")
	}
}
