package store

import (
	"path/filepath"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestDeadLetterRoundTrip(t *testing.T) {
	s, _ := openTemp(t)

	if n, err := s.DeadLetterLen(); err != nil || n != 0 {
		t.Fatalf("DeadLetterLen on empty = %d (err %v), want 0", n, err)
	}

	dl := DeadLetter{
		Type:       OpDownload,
		Direction:  DirInbound,
		ViewID:     7,
		Path:       "docs/a.txt",
		FileID:     "fid-1",
		Reason:     "unappliable: bad path",
		RecordedAt: 1_700_000_000,
	}
	seq, err := s.RecordDeadLetter(dl)
	if err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}
	if seq == 0 {
		t.Errorf("RecordDeadLetter returned seq 0, want a monotonic seq")
	}

	n, err := s.DeadLetterLen()
	if err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	}
	if n != 1 {
		t.Fatalf("DeadLetterLen = %d, want 1", n)
	}

	got, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("ListDeadLetters len = %d, want 1", len(got))
	}
	if got[0].Seq != seq {
		t.Errorf("seq = %d, want %d", got[0].Seq, seq)
	}
	if !reflect.DeepEqual(got[0].DeadLetter, dl) {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got[0].DeadLetter, dl)
	}
}

func TestDeadLetterOrderedByRecording(t *testing.T) {
	s, _ := openTemp(t)

	for i, p := range []string{"first", "second", "third"} {
		if _, err := s.RecordDeadLetter(DeadLetter{ViewID: 1, Path: p, RecordedAt: int64(i)}); err != nil {
			t.Fatalf("RecordDeadLetter %q: %v", p, err)
		}
	}
	got, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	want := []string{"first", "second", "third"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Path != w {
			t.Errorf("record %d path = %q, want %q", i, got[i].Path, w)
		}
	}
}

// TestDeadLetterRingCap confirms the bucket is a FIFO ring: recording more than
// maxDeadLetters keeps exactly maxDeadLetters records, retaining the most recent
// (highest seq) and pruning the oldest.
func TestDeadLetterRingCap(t *testing.T) {
	s, _ := openTemp(t)

	const extra = 25
	total := maxDeadLetters + extra
	for i := 0; i < total; i++ {
		if _, err := s.RecordDeadLetter(DeadLetter{ViewID: 1, Path: "p", RecordedAt: int64(i)}); err != nil {
			t.Fatalf("RecordDeadLetter %d: %v", i, err)
		}
	}

	n, err := s.DeadLetterLen()
	if err != nil {
		t.Fatalf("DeadLetterLen: %v", err)
	}
	if n != maxDeadLetters {
		t.Fatalf("DeadLetterLen = %d, want %d", n, maxDeadLetters)
	}

	got, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != maxDeadLetters {
		t.Fatalf("ListDeadLetters len = %d, want %d", len(got), maxDeadLetters)
	}
	// The oldest `extra` records (RecordedAt 0..extra-1) were pruned; the surviving
	// set is RecordedAt extra..total-1, in ascending seq order.
	if first := got[0].RecordedAt; first != extra {
		t.Errorf("oldest surviving RecordedAt = %d, want %d", first, extra)
	}
	if last := got[len(got)-1].RecordedAt; last != int64(total-1) {
		t.Errorf("newest surviving RecordedAt = %d, want %d", last, total-1)
	}
	// Seqs must remain strictly ascending (monotonic, never reset by pruning).
	for i := 1; i < len(got); i++ {
		if got[i].Seq <= got[i-1].Seq {
			t.Fatalf("seq not ascending at %d: %d <= %d", i, got[i].Seq, got[i-1].Seq)
		}
	}
}

// TestRecordDeadLetterAndAckIsAtomic confirms RecordDeadLetterAndAck records the trace
// AND removes the queued op in one transaction: after the call the dead-letter trace is
// present and the queue no longer holds the op. Atomicity is what stops a retry from
// re-recording the same trace - the trace exists iff the op left the queue.
func TestRecordDeadLetterAndAckIsAtomic(t *testing.T) {
	s, _ := openTemp(t)

	seq, err := s.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "a.txt", FileID: "f1"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if n, err := s.PendingLen(); err != nil || n != 1 {
		t.Fatalf("PendingLen before = %d (err %v), want 1", n, err)
	}

	dlSeq, err := s.RecordDeadLetterAndAck(DeadLetter{
		Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "a.txt", FileID: "f1",
		Reason: "unappliable", RecordedAt: 1_700_000_000,
	}, seq, nil, nil)
	if err != nil {
		t.Fatalf("RecordDeadLetterAndAck: %v", err)
	}
	if dlSeq == 0 {
		t.Errorf("RecordDeadLetterAndAck returned seq 0, want a monotonic seq")
	}

	// The trace is durable...
	if n, err := s.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 1", n, err)
	}
	// ...and the queue op is gone in the same transaction.
	if n, err := s.PendingLen(); err != nil || n != 0 {
		t.Fatalf("PendingLen after = %d (err %v), want the op acked (0)", n, err)
	}

	// Acking an absent seq is a no-op (matching Ack), so a caller that retries after a
	// cleanup failure does not error - it just records another trace and the (already
	// gone) op stays gone.
	if _, err := s.RecordDeadLetterAndAck(DeadLetter{ViewID: 7, Path: "a.txt"}, 9999, nil, nil); err != nil {
		t.Fatalf("RecordDeadLetterAndAck with absent seq: %v", err)
	}
}

// TestRecordDeadLetterAndAckEnqueuesCompensation confirms the compensating op rides in the SAME
// transaction as the trace and the Ack (two-client finding H2): the dropped op leaves the queue,
// the trace is recorded, and the replacement op is appended at the tail under a fresh monotonic
// seq - so a drop can never commit without its repair, nor the repair without the drop.
func TestRecordDeadLetterAndAckEnqueuesCompensation(t *testing.T) {
	s, _ := openTemp(t)

	// An op already queued ahead of the dropped one, so the compensation's tail placement is
	// observable in FIFO order.
	if _, err := s.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "first.txt", FileID: "f0"}); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	seq, err := s.Enqueue(Op{
		Type: OpRename, Direction: DirInbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f1",
	})
	if err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	comp := Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "b.txt", FileID: "f1"}
	if _, err := s.RecordDeadLetterAndAck(DeadLetter{
		Type: OpRename, Direction: DirInbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt",
		FileID: "f1", Reason: "rename source missing or foreign", RecordedAt: 1_700_000_000,
	}, seq, &comp, nil); err != nil {
		t.Fatalf("RecordDeadLetterAndAck with compensation: %v", err)
	}

	if n, err := s.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 1", n, err)
	}
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("PendingOps = %+v, want the untouched first op plus the compensation", ops)
	}
	if ops[0].Path != "first.txt" {
		t.Errorf("head op = %+v, want the pre-existing first.txt untouched", ops[0])
	}
	tail := ops[1]
	if tail.Seq <= seq {
		t.Errorf("compensation seq = %d, want > the acked %d (appended at the tail)", tail.Seq, seq)
	}
	if tail.Type != OpDownload || tail.Direction != DirInbound || tail.Path != "b.txt" || tail.FileID != "f1" {
		t.Errorf("compensation = %+v, want an inbound download of f1 at b.txt", tail.Op)
	}
}

// TestListDeadLettersSkipsCorruptValue confirms a bit-rotted dead-letter value
// between healthy siblings does not abort the whole listing (the poison-pill
// shape already removed from the other
// per-record readers): the healthy records still list in order and the corrupt
// one is skipped with a Warn, left on disk for the ring's pruning to retire.
func TestListDeadLettersSkipsCorruptValue(t *testing.T) {
	s, _ := openTemp(t)

	if _, err := s.RecordDeadLetter(DeadLetter{ViewID: 1, Path: "first", RecordedAt: 1}); err != nil {
		t.Fatalf("RecordDeadLetter first: %v", err)
	}
	// Inject a corrupt value between the healthy siblings, under a real
	// NextSequence-allocated key so ordering and pruning semantics hold.
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDeadLetters)
		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		return b.Put(encodeUint64(seq), []byte("{not valid json"))
	})
	if err != nil {
		t.Fatalf("inject corrupt dead-letter: %v", err)
	}
	if _, err := s.RecordDeadLetter(DeadLetter{ViewID: 1, Path: "third", RecordedAt: 3}); err != nil {
		t.Fatalf("RecordDeadLetter third: %v", err)
	}

	got, err := s.ListDeadLetters()
	if err != nil {
		t.Fatalf("ListDeadLetters with corrupt record: %v (want success - listing must skip it)", err)
	}
	want := []string{"first", "third"}
	if len(got) != len(want) {
		t.Fatalf("ListDeadLetters len = %d, want %d (healthy records only)", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Path != w {
			t.Errorf("record %d path = %q, want %q", i, got[i].Path, w)
		}
	}
	// The corrupt record stays on disk (recoverable, retired by the ring), so
	// DeadLetterLen still counts it.
	if n, err := s.DeadLetterLen(); err != nil || n != 3 {
		t.Fatalf("DeadLetterLen = %d (err %v), want 3 (corrupt record left in place)", n, err)
	}
}

// TestMigrateV5ToV6AddsDeadLetters confirms reopening a simulated v5 database (the
// deadletters bucket absent, schema_version stamped to 5) migrates up to v6 and the
// dead-letter API is usable afterwards.
func TestMigrateV5ToV6AddsDeadLetters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Downgrade the on-disk state to a simulated v5: drop the deadletters bucket and
	// stamp schema_version=5.
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketDeadLetters); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(5))
	})
	if err != nil {
		t.Fatalf("simulate v5: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}
	if _, err := s2.RecordDeadLetter(DeadLetter{ViewID: 1, Path: "x", Reason: "r"}); err != nil {
		t.Fatalf("RecordDeadLetter after migrate: %v", err)
	}
	if n, err := s2.DeadLetterLen(); err != nil || n != 1 {
		t.Fatalf("DeadLetterLen after migrate = %d (err %v), want 1", n, err)
	}
}

// TestRecordDeadLetterAndAckWritesTrackedBaseline pins the third payload of the one transaction:
// the StatePending record that ARMS the compensating op's never-lose gate (engine finding H2).
// The engine covers the behaviour end to end, but the byid consistency it rests on is the store's
// own invariant ("composite ops are atomic in one transaction so byid never drifts"), and the
// TrackedBaseline arm had no store-level test at all - every other call site passes nil.
func TestRecordDeadLetterAndAckWritesTrackedBaseline(t *testing.T) {
	s, _ := openTemp(t)

	seq, err := s.Enqueue(Op{
		Type: OpRename, Direction: DirInbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt", FileID: "f1",
	})
	if err != nil {
		t.Fatalf("Enqueue rename: %v", err)
	}

	comp := Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "b.txt", FileID: "f1"}
	track := TrackedBaseline{
		ViewID: 7, Path: "b.txt",
		Base: Baseline{FileID: "f1", FileType: "file", ServerSyncID: 42, State: StatePending},
	}
	if _, err := s.RecordDeadLetterAndAck(DeadLetter{
		Type: OpRename, Direction: DirInbound, ViewID: 7, Path: "a.txt", NewPath: "b.txt",
		FileID: "f1", Reason: "rename source missing or foreign", RecordedAt: 1_700_000_000,
	}, seq, &comp, &track); err != nil {
		t.Fatalf("RecordDeadLetterAndAck with a tracked baseline: %v", err)
	}

	b, err := s.GetBaseline(7, "b.txt")
	if err != nil {
		t.Fatalf("GetBaseline(b.txt): %v", err)
	}
	if b.FileID != "f1" || b.State != StatePending || b.ServerSyncID != 42 {
		t.Errorf("baseline = %+v, want the pending record the compensation's gate reads", b)
	}
	// The byid index must be written in the SAME transaction, or the record is invisible to every
	// id-addressed lookup the never-lose gates make.
	if p, err := s.PathByID(7, "f1"); err != nil || p != "b.txt" {
		t.Errorf("PathByID(f1) = %q (err %v), want b.txt", p, err)
	}
	if n, err := s.DeadLetterLen(); err != nil || n != 1 {
		t.Errorf("DeadLetterLen = %d (err %v), want the trace committed with it", n, err)
	}
	ops, err := s.PendingOps()
	if err != nil {
		t.Fatalf("PendingOps: %v", err)
	}
	if len(ops) != 1 || ops[0].Path != "b.txt" || ops[0].Seq <= seq {
		t.Errorf("PendingOps = %+v, want only the appended compensation (the rename acked)", ops)
	}
}
