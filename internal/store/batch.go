package store

// Batched writes: many queue records and their baseline mutations in ONE transaction.
//
// The durable queue's contract is that a queue record is written BEFORE the baseline
// mutation it owns, so a crash between the two leaves a replayable op rather than a pending
// baseline nothing will resolve. That is a statement about ORDER, not about transaction
// granularity: a single transaction that writes many records, each ahead of its own
// baseline, satisfies it exactly as a transaction per record does - and a transaction is
// all-or-nothing, so a crash discards a whole batch, which is a batch that never happened.
//
// The granularity matters because bolt fsyncs per transaction and rewrites a b-tree path
// plus the freelist each time. Measured on a real cold start of a 190k-node view: two
// transactions per planned op (the record, then its baseline) ran at 40 ops/s and wrote
// 47.6 GB of physical I/O into a database that ended up 241 MB - roughly 348 KB per record,
// for records measured in bytes. The whole first sync spent over an hour there with the
// network idle.

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Writer is the subset of the store a planned op's apply step needs.
//
// It exists so one apply implementation serves both granularities: *Store gives each call
// its own transaction, which is what the incremental path wants (a handful of ops, latency
// over throughput), and *Batch puts them all in one, which is what a cold start's hundreds
// of thousands want. Neither is a degraded version of the other, and the apply step cannot
// tell them apart.
type Writer interface {
	PutBaseline(viewID uint64, path string, b Baseline) error
	MoveBaseline(viewID uint64, fromPath, toPath string, b Baseline) error
	GetBaseline(viewID uint64, path string) (Baseline, error)
	// PathByID resolves a node's current path. It is part of the seam because a caller that
	// must write the queue record and the baseline it arms in ONE transaction (reconcile's
	// enqueue/markPending pair) has to resolve the node inside that transaction too - reaching
	// back to the *Store from inside a Batch is the documented deadlock below.
	PathByID(viewID uint64, fileID string) (string, error)
}

// Batch accumulates writes inside one transaction. It is only valid for the duration of the
// Batch call that produced it; using it afterwards panics on a closed transaction rather
// than silently losing the write.
type Batch struct {
	tx *bolt.Tx
}

// Batch runs fn with a Writer whose every write lands in ONE transaction, committed when fn
// returns nil and discarded whole when it returns an error.
//
// A caller that needs the queue record ahead of its baseline still writes them in that
// order inside fn; the ordering guarantee is unchanged, and the atomicity is stronger.
//
// fn must use the *Batch it is given for every store access. Reaching back for a *Store method
// deadlocks bolt's writer lock permanently, with no timeout and no error to diagnose it, so the
// calling goroutine is recorded for the duration and any such call panics with the remedy
// instead (reentrancy.go).
func (s *Store) Batch(fn func(*Batch) error) error {
	return s.update(func(tx *bolt.Tx) error {
		leave := s.batch.enter()
		defer leave()
		return fn(&Batch{tx: tx})
	})
}

// Enqueue appends an op to the durable queue inside the batch's transaction, returning its
// sequence. The sequence comes from the same counter a standalone Enqueue uses, so batched
// and unbatched ops interleave in one order - the only order the executor reads.
func (b *Batch) Enqueue(op Op) (uint64, error) {
	enc, err := json.Marshal(op)
	if err != nil {
		return 0, fmt.Errorf("store: encode op: %w", err)
	}
	return enqueueTx(b.tx, enc)
}

// PutBaseline writes a baseline inside the batch's transaction.
func (b *Batch) PutBaseline(viewID uint64, path string, bl Baseline) error {
	enc, err := json.Marshal(bl)
	if err != nil {
		return fmt.Errorf("store: encode baseline %d/%q: %w", viewID, path, err)
	}
	return putBaselineTx(b.tx, viewID, path, enc, bl.FileID)
}

// MoveBaseline relocates a baseline inside the batch's transaction.
func (b *Batch) MoveBaseline(viewID uint64, fromPath, toPath string, bl Baseline) error {
	if fromPath == toPath {
		return b.PutBaseline(viewID, toPath, bl)
	}
	enc, err := json.Marshal(bl)
	if err != nil {
		return fmt.Errorf("store: encode baseline %d/%q: %w", viewID, toPath, err)
	}
	return moveBaselineTx(b.tx, viewID, fromPath, toPath, enc, bl.FileID)
}

// GetBaseline reads a baseline inside the batch's transaction, so an apply step sees the
// writes its own batch has already made - exactly as it would see committed writes when run
// one transaction at a time.
func (b *Batch) GetBaseline(viewID uint64, path string) (Baseline, error) {
	return getBaselineTx(b.tx, viewID, path)
}

// DeleteBaseline drops a baseline and its byid entry inside the batch's transaction.
//
// It exists for the SUBTREE mutations: a directory the server deleted or moved retires or
// relocates one baseline per descendant, and at one transaction each a large subtree pays an
// fsync per record on the sync loop's own goroutine. Same body as the standalone
// DeleteBaseline (deleteBaselineTx), including the corrupt-record index sweep, so a batched
// drop and an unbatched one differ only in how many commits they cost.
func (b *Batch) DeleteBaseline(viewID uint64, path string) error {
	return deleteBaselineTx(b.tx, viewID, path)
}

// PathByID resolves a node's path inside the batch's transaction, seeing the batch's own
// index writes for the same reason GetBaseline sees its own baseline writes.
func (b *Batch) PathByID(viewID uint64, fileID string) (string, error) {
	return pathByIDTx(b.tx, viewID, fileID)
}
