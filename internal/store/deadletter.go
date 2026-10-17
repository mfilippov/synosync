package store

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// DeadLetter is the durable record of work the engine will not do - usually one queued
// operation it dropped rather than applied: a structurally invalid server path, a
// pre-existing symlink along the on-disk location, a local non-directory blocking a path
// component, or a rename whose feeding op was itself dead-lettered (DESIGN §6). One class
// never had a queued op at all: two on-disk names folding to a single sync key, which the
// path-keyed queue cannot even represent. It carries OpFoldCollision, is written by the bare
// RecordDeadLetter, and its caller owns the idempotence a queue Ack would otherwise give it.
// Without it a
// dead-letter left only an Ack (the op vanished from the queue) plus a one-shot Warn
// in the log, so an operator inspecting a running daemon had no way to see that data
// had been silently skipped. The record is the durable trace: it survives a restart
// and is surfaced as a count in `status`. Values are JSON-encoded in the deadletters
// bucket under a monotonic big-endian sequence key allocated by NextSequence.
type DeadLetter struct {
	// Type is the dropped op's type, or OpFoldCollision for a record that stands for
	// skipped work no op ever represented.
	Type OpType `json:"type"`
	// Direction is the dropped op's side (inbound to disk, outbound to server).
	Direction Direction `json:"direction,omitempty"`
	// ViewID is the view the dropped op targeted.
	ViewID uint64 `json:"view_id"`
	// Path is the view-relative path of the dropped op (the source path for a rename).
	// Empty for the fold-collision OVERFLOW summary, which stands for a set of keys rather
	// than one path - and is told apart from a real trace by exactly that emptiness, since
	// the engine's memo can only seed itself from a record that names a key.
	Path string `json:"path"`
	// NewPath is the destination path for a dropped rename; empty otherwise.
	NewPath string `json:"new_path,omitempty"`
	// FileID is the server node id the dropped op addressed, when known.
	FileID string `json:"file_id,omitempty"`
	// Reason is a short human-readable explanation of why the op was dropped.
	Reason string `json:"reason"`
	// RecordedAt is the unix time (seconds) the op was dead-lettered.
	RecordedAt int64 `json:"recorded_at"`
}

// DeadLetteredOp pairs a DeadLetter with the sequence number it is stored under.
// Ascending seq is the order it was recorded in.
type DeadLetteredOp struct {
	Seq uint64
	DeadLetter
}

// maxDeadLetters bounds the dead-letter bucket so a long-lived daemon that keeps
// dropping the same broken path cannot grow the trace without limit. The bucket is a
// FIFO ring: once a Put pushes the count past this cap, recordDeadLetterTx trims the
// oldest records (lowest seq) in the same transaction, keeping the most recent
// maxDeadLetters. Records are tiny and dropping is rare, so this only ever bites a
// pathologically wedged install.
const maxDeadLetters = 1000

// TrackedBaseline is a baseline write that must commit in the SAME transaction as a
// dead-lettered drop's trace, ack and compensating op: the pending record the compensation's
// own never-lose gate reads. See RecordDeadLetterAndAck for why neither ordering outside the
// transaction is safe.
type TrackedBaseline struct {
	ViewID uint64
	Path   string
	Base   Baseline
}

// RecordDeadLetterAndAck atomically appends dl to the dead-letter bucket, enqueues the
// optional compensation op AND removes the queued op with ackSeq from the queue, in one
// Update transaction. The trace and the ack MUST be atomic: if they were separate writes
// and the ack failed (or the process crashed)
// after the trace was written, the op would stay queued, the next drain would re-enter its
// dead-letter branch and record the same op's trace again, double-counting the `status`
// Dropped figure. Doing both in one transaction means the trace exists iff the
// op left the queue, so a retry can never re-record it. Acking an absent seq is a no-op
// (matching Ack), so the caller may safely retry after a cleanup failure. The
// FIFO-ring trimming of recordDeadLetterTx applies.
//
// A failure here is NOT best-effort and must NOT be swallowed. The transaction no longer carries
// only the trace: it carries the compensating op and the baseline that arms that op's never-lose
// gate (below), so a caller that logged-and-continued would report a drop it did not perform -
// acking nothing while the op stays queued, or, worse, treating the repair as enqueued when it is
// not. Every caller therefore wraps and RETURNS the error and leaves the op queued for the next
// drain (engine's deadLetterCompensateAndAck and its drop sites); re-running is safe because the
// whole write is one transaction and the engine's cleanup helpers are idempotent. The one drop
// class with no queued op to ack has its own primitive (RecordDeadLetter below), which is where
// the best-effort-observability reading does belong.
//
// compensation (nil for every drop that needs none) is the op the drop must leave behind so
// the dropped work is not simply lost - the inbound rename whose moved node is absent from
// disk re-requests that node at its destination (two-client finding H2). It rides in the SAME
// transaction for the same reason the trace does: a separate Enqueue could commit twice on a
// retry (a duplicate transfer) or not at all (the drop stays unrepaired, which is the very
// permanent-divergence the compensation exists to close). It is appended at the queue tail
// under a fresh monotonic seq, so it drains after everything already queued - including the
// ops the dropped one was serialised against.
//
// track (nil for every drop that needs none) is the baseline the compensation's own never-lose
// gate reads, and it rides in the same transaction for a third reason: written BEFORE the
// commit it would survive a failed or crashed drop, and the record it leaves - a byid-indexed
// claim that the moved node lives at the rename's destination - is exactly what disarms
// applyRename's foreign-source guards when the still-queued rename replays. Written AFTER, a
// crash would leave the compensation queued with its gate unarmed, so the repair could clobber
// whatever occupies the destination. Only inside the transaction is neither possible.
func (s *Store) RecordDeadLetterAndAck(dl DeadLetter, ackSeq uint64, compensation *Op, track *TrackedBaseline) (uint64, error) {
	enc, err := json.Marshal(dl)
	if err != nil {
		return 0, fmt.Errorf("store: encode dead-letter: %w", err)
	}
	// Encode the compensation outside the transaction, mirroring Enqueue: a marshal failure
	// must abort before anything is written, never mid-transaction.
	var compEnc []byte
	if compensation != nil {
		compEnc, err = json.Marshal(*compensation)
		if err != nil {
			return 0, fmt.Errorf("store: encode compensating op: %w", err)
		}
	}
	var trackEnc []byte
	if track != nil {
		trackEnc, err = json.Marshal(track.Base)
		if err != nil {
			return 0, fmt.Errorf("store: encode tracked baseline %d/%q: %w", track.ViewID, track.Path, err)
		}
	}
	var seq uint64
	err = s.update(func(tx *bolt.Tx) error {
		seq, err = recordDeadLetterTx(tx, enc)
		if err != nil {
			return err
		}
		q := tx.Bucket(bucketQueue)
		if err := q.Delete(encodeUint64(ackSeq)); err != nil {
			return fmt.Errorf("store: ack dead-lettered %d: %w", ackSeq, err)
		}
		if compEnc != nil {
			n, err := q.NextSequence()
			if err != nil {
				return fmt.Errorf("store: compensation next sequence: %w", err)
			}
			if err := q.Put(encodeUint64(n), compEnc); err != nil {
				return fmt.Errorf("store: enqueue compensation %d: %w", n, err)
			}
		}
		if trackEnc == nil {
			return nil
		}
		return putBaselineTx(tx, track.ViewID, track.Path, trackEnc, track.Base.FileID)
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// RecordDeadLetter appends dl to the durable dead-letter bucket on its own - no queue ack,
// no compensation - and returns the monotonic sequence number assigned to it, with
// recordDeadLetterTx's FIFO-ring trimming.
//
// Every op-shaped drop must use RecordDeadLetterAndAck instead: the trace has to exist iff
// the op left the queue, or a retried drop re-records it and double-counts `status` Dropped
// figure. This bare append exists for the one class of skipped work that never had a
// queued op to ack - two on-disk names folding to one sync key, which the store cannot even
// key separately, so the engine could neither enqueue nor track it and the file was silently
// left unsynced forever (two-client finding H6). Its caller is responsible for not re-recording
// the same condition on every scan pass (engine's collisionNoted).
func (s *Store) RecordDeadLetter(dl DeadLetter) (uint64, error) {
	enc, err := json.Marshal(dl)
	if err != nil {
		return 0, fmt.Errorf("store: encode dead-letter: %w", err)
	}
	var seq uint64
	err = s.update(func(tx *bolt.Tx) error {
		seq, err = recordDeadLetterTx(tx, enc)
		return err
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// recordDeadLetterTx puts the already-encoded dead-letter record into the bucket under a
// fresh monotonic seq and trims the FIFO ring back to maxDeadLetters, all within the
// caller's transaction. It is the transactional core of both RecordDeadLetterAndAck and the
// bare RecordDeadLetter above, so both share one definition of the put-plus-trim invariant.
func recordDeadLetterTx(tx *bolt.Tx, enc []byte) (uint64, error) {
	b := tx.Bucket(bucketDeadLetters)
	seq, err := b.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("store: dead-letter next sequence: %w", err)
	}
	if err := b.Put(encodeUint64(seq), enc); err != nil {
		return 0, fmt.Errorf("store: record dead-letter %d: %w", seq, err)
	}
	// Trim oldest back down to the cap. Seqs are monotonic (NextSequence never
	// resets) and the bucket is contiguous from prior pruning, so any key whose
	// seq is at or below seq-maxDeadLetters is older than the most recent
	// maxDeadLetters and is dropped. Bucket.Stats().KeyN can't be used here - it
	// doesn't reflect this transaction's own uncommitted Put/Delete. Collect the
	// doomed keys first, then delete: mutating under a live cursor (Delete + Next)
	// skips the element after each removal in bbolt.
	if seq > maxDeadLetters {
		threshold := seq - maxDeadLetters
		var doomed [][]byte
		c := b.Cursor()
		for k, _ := c.First(); k != nil && decodeUint64(k) <= threshold; k, _ = c.Next() {
			doomed = append(doomed, append([]byte(nil), k...))
		}
		for _, k := range doomed {
			if err := b.Delete(k); err != nil {
				return 0, fmt.Errorf("store: prune dead-letter %d: %w", decodeUint64(k), err)
			}
		}
	}
	return seq, nil
}

// DeadLetterLen returns the number of dead-letter records currently retained.
func (s *Store) DeadLetterLen() (int, error) {
	var n int
	err := s.view(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketDeadLetters).Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// ListDeadLetters returns every dead-letter record in ascending sequence (recording)
// order. A record whose value does not decode (bit rot) is skipped with a Warn
// rather than aborting the whole listing - mirroring ListViews/ListConflicts:
// the healthy records still list, the corrupt one stays on
// disk for inspection, and the FIFO ring's pruning retires it naturally.
func (s *Store) ListDeadLetters() ([]DeadLetteredOp, error) {
	var out []DeadLetteredOp
	err := s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDeadLetters).Cursor()
		for k, raw := c.First(); k != nil; k, raw = c.Next() {
			var dl DeadLetter
			if err := json.Unmarshal(raw, &dl); err != nil {
				s.log().Warn("store: skipping corrupt dead-letter record", "seq", decodeUint64(k), "error", err)
				continue
			}
			out = append(out, DeadLetteredOp{Seq: decodeUint64(k), DeadLetter: dl})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
