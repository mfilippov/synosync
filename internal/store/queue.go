package store

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// OpType enumerates the kinds of operation the sync engine (phase 6) records in
// the durable queue. The strings are stable on-disk values. An OpType names the
// kind of change (a file transfer, a directory create, a rename, a remove); the
// side it is applied to - local disk or the server - is carried separately by
// Direction, so e.g. an OpDownload+DirInbound pulls a server file to disk while an
// OpUpload+DirOutbound pushes a local file to the server.
type OpType string

const (
	// OpUpload transfers a created or modified file (server→disk is unused; the
	// outbound push uses this for disk→server).
	OpUpload OpType = "upload"
	// OpDownload transfers a created or modified file (inbound pulls a server file
	// to disk).
	OpDownload OpType = "download"
	// OpMkdir creates a directory.
	OpMkdir OpType = "mkdir"
	// OpRename moves/renames a node from Path to NewPath.
	OpRename OpType = "rename"
	// OpRemove deletes a node.
	OpRemove OpType = "remove"
	// OpResolveConflict resolves a keep-both content conflict: the server version
	// wins on the canonical path and the divergent local content is preserved as a
	// _Conflict copy pushed back to the server (DESIGN §6). It is enqueued by the
	// outbound stale-base (24579) funnel and applied direction-agnostically.
	OpResolveConflict OpType = "resolve_conflict"
	// OpFoldCollision never names a QUEUED op - the path-keyed queue cannot even
	// represent the thing it describes, which is two on-disk names sharing one sync
	// key. It exists only as the Type of the bare DeadLetter the engine writes for a
	// fold-key collision (engine/collisiontrace.go), so that record can be recognised
	// in the durable ring by a typed value rather than by matching a substring of its
	// operator-facing Reason prose. Enqueue never produces it; readers that switch on
	// an op type simply never see it.
	OpFoldCollision OpType = "fold_collision"
	// OpUnrepresentableName, like OpFoldCollision, never names a queued op: it is the Type of
	// the bare DeadLetter for a server name the local filesystem cannot hold
	// (engine/unrepresentable.go), recognised in the ring by this value.
	OpUnrepresentableName OpType = "unrepresentable_name"
)

// Direction is the side an Op is applied to. The remote→local (inbound) path
// enqueues DirInbound ops (Phase 6b) and the local→server (outbound) push side
// enqueues DirOutbound ops (Phase 6c). The string values are stable on-disk; an
// absent/empty value (older records, or before the field existed) decodes as
// DirInbound's zero string and must be read as "unset".
type Direction string

const (
	// DirInbound applies the operation to local disk (server is the authority).
	DirInbound Direction = "inbound"
	// DirOutbound applies the operation to the server (local disk is the source).
	DirOutbound Direction = "outbound"
)

// Op is one durable sync operation (its Direction names the side - inbound to
// local disk or outbound to the server). It is enqueued by the reconcile pass and
// drained by the transfer worker; surviving a restart is the whole point of
// persisting it, so a crash mid-sync resumes from the queue rather than re-walking
// state. Values are JSON-encoded in the queue bucket under a monotonic big-endian
// sequence key allocated by NextSequence, which keeps FIFO order on the wire.
type Op struct {
	// Type selects which action the worker performs.
	Type OpType `json:"type"`
	// Direction is the side the operation is applied to (inbound = local disk,
	// outbound = server). The field is additive and omitempty, so older records
	// without it decode to the empty string.
	Direction Direction `json:"direction,omitempty"`
	// ViewID is the view the operation targets.
	ViewID uint64 `json:"view_id"`
	// Path is the view-relative path of the node (the source path for a rename).
	Path string `json:"path"`
	// NewPath is the destination view-relative path for an OpRename; empty
	// otherwise.
	NewPath string `json:"new_path,omitempty"`
	// IsDir records whether the moved node is a directory, threaded from the move
	// event (ev.IsDir) onto an inbound OpRename. The on-disk source kind alone cannot
	// tell our never-materialised node from unrelated user content occupying the source
	// path, so applyRename's foreign-source guard compares this declared kind against
	// the on-disk source and drops the rename on a mismatch (a file dragged onto a
	// server directory's path, or vice versa) rather than mislabelling the content as
	// op.FileID. False (the default, and every non-rename op) means "file"; an op
	// produced before this field existed decodes to false, preserving prior behaviour.
	IsDir bool `json:"is_dir,omitempty"`
	// FileID is the server node id when known (renames/removes/downloads address
	// an existing node); empty for a fresh upload.
	FileID string `json:"file_id,omitempty"`
	// ServerSyncID is the sync id of the event that produced this op, recorded into
	// the baseline's ServerSyncID when the op is applied. It lets a create/modify
	// (download/mkdir) carry the event's sync id through to the synced baseline even
	// when an earlier dead-lettered op for the same node has dropped the pending
	// baseline reconcile pre-wrote, so the rebuilt baseline does not lose it.
	//
	// An inbound OpRemove and OpRename carry it too, as a POSITION rather than a stamp to
	// record. On a remove it is the delete's own position, and removeSupersededByLaterObservation
	// refuses the delete when the path - or any descendant - holds a baseline stamped strictly
	// ABOVE it, which proves a successor node the delete never named stands there (finding H3).
	// On a rename it is stamped onto the compensating download a dropped rename leaves behind, so
	// the re-requested node keeps the provenance the rename would have given it. Both guards
	// disarm themselves at 0, so an op enqueued before this field was carried keeps the
	// historical behaviour.
	ServerSyncID int64 `json:"server_sync_id,omitempty"`
	// LocalHash is the MD4 content hash (lowercase hex) the operation expects to
	// transfer, used to skip no-op transfers and to verify completion.
	LocalHash string `json:"local_hash,omitempty"`
	// LocalSize is the content length in bytes when relevant.
	LocalSize int64 `json:"local_size,omitempty"`
	// LocalMTime is the local modification timestamp when relevant.
	LocalMTime int64 `json:"local_mtime,omitempty"`
	// UnixPerm carries the permission bits to set when relevant.
	UnixPerm uint32 `json:"unix_perm,omitempty"`
	// BaseLocalHash is the last-synced local content hash of an inbound OpDownload's target,
	// captured at reconcile time. applyDownload re-hashes the on-disk file against it before
	// overwriting to detect a local edit the cheap scan (size+mtime short-circuit, Phase 9d)
	// missed and route it to the keep-both resolver instead of silently clobbering it. Empty (a
	// fresh create, or an op produced before this field existed) skips the check, preserving
	// prior behaviour.
	//
	// An OUTBOUND OpUpload carries the same capture for the mirror-image reason: it is the
	// content the push is written against, and guardUploadTarget compares the SERVER's current
	// hash to it when the baseline's ServerHash is empty (the upload reply that produced it
	// carried no hash). Only a modify off a StateSynced record sets it there.
	BaseLocalHash string `json:"base_local_hash,omitempty"`
	// DestBaseLocalHash is the last-synced local hash of the FOREIGN node an inbound OpRename's
	// destination held before the move, captured at reconcile time (before relocatePending
	// overwrites that destination baseline). applyRename re-hashes the on-disk destination
	// against it: a divergence means a local edit the cheap scan missed, so the destination is
	// set aside as a keep-both copy rather than silently clobbered by the move. Empty means the
	// destination was untracked (see DestUntracked) or carried no recorded local hash (server
	// authority - no divergence to detect).
	DestBaseLocalHash string `json:"dest_base_local_hash,omitempty"`
	// DestUntracked records that an inbound OpRename's destination had NO baseline at reconcile
	// time. A regular file found there at apply time is therefore user content the server never
	// saw, so applyRename preserves it as a keep-both copy instead of letting the move clobber
	// it. False (the default, and the common clean-move case where the destination is empty on
	// disk) preserves prior behaviour.
	DestUntracked bool `json:"dest_untracked,omitempty"`
	// BaseWasSynced records that an inbound OpRemove's target baseline was StateSynced at reconcile
	// time, captured BEFORE markPending->flipPending flips it to StatePending (which would otherwise
	// erase the distinction). Only a synced baseline carries a trustworthy last-synced LocalHash, so
	// applyRemove's guardRemoveDivergence treats BaseLocalHash as a real divergence base only when
	// this is set. False (a non-synced/untracked target, an op produced before this field existed, or
	// any non-remove op) makes the guard preserve a present regular file whole as a keep-both copy
	// rather than trusting a possibly-divergent LocalHash as its own base - the safe, never-lose
	// default.
	//
	// An OUTBOUND OpUpload carries it as the POSITIVE discriminator between a create and a modify
	// (uploadIsCreate): only detectCreatesAndModifies' modify arm holds a StateSynced record, so
	// only it sets this. The absence of facts cannot answer that question - a synced record can
	// legitimately carry neither a FileID nor a LocalHash (an inbound rename recording bare facts
	// for an id-less node, revertRelocatedOutbound clearing the local facts of a re-emitted push),
	// and reading such a modify as a create hands it the OCCUPANCY guard, which the node's own
	// server-side version always trips - funnelling an ordinary edit into a spurious _Conflict copy.
	// False (an op produced before this field existed) keeps the historical fact-based reading.
	BaseWasSynced bool `json:"base_was_synced,omitempty"`
	// BaseIsDir records that an OpRemove's reconcile-time baseline was a DIRECTORY.
	//
	// On an OUTBOUND remove it is the durable copy of the delete target's kind, which arms the
	// recursive-delete subtree precondition (verifyServerSubtreeAgrees, finding H7) even when the
	// record itself no longer exists at apply time - the baseline is the only other place the kind
	// lives, and reading an absent record as "not a directory" would issue the RECURSIVE server
	// delete with nothing proved about the subtree.
	//
	// On an INBOUND OpRemove it describes the reconcile-time baseline BaseWasSynced also
	// describes. A directory baseline never carries a content hash, so without
	// this a regular file that locally replaced the tracked directory (rm -rf d && echo data > d)
	// would fall into removeBaseFor's "synced without hash: server authority" branch and be
	// destroyed with no _Conflict copy - the empty hash is a structural fact of the dir record,
	// not evidence the on-disk file matches anything the server saw. Like
	// DestUntracked/BaseWasSynced, the field is additive with a tolerant decode on both sides
	// (queue records use plain encoding/json, which ignores unknown fields), so no
	// CurrentSchemaVersion bump is needed; an op produced before it existed decodes to false,
	// preserving prior behaviour.
	BaseIsDir bool `json:"base_is_dir,omitempty"`
	// MovedBaseSynced records that an inbound OpRename's MOVED node was StateSynced at reconcile
	// time, captured from the pre-batch snapshot BEFORE relocatePending flips the relocated
	// baseline to StatePending (which keeps LocalHash but erases the state). Together with
	// MovedBaseLocalHash it lets a download for the same node reconciled in a LATER batch (before
	// any drain) inherit the node's last-synced divergence base from the queued rename - without
	// it, a cross-batch [move a->b] + [modify b] carries an empty base and applyDownload clobbers
	// a cheap-scan-missed local edit the rename carried to b. Like
	// DestUntracked/BaseWasSynced, the field is additive with a tolerant decode on both sides
	// (queue records use plain encoding/json, which ignores unknown fields), so no
	// CurrentSchemaVersion bump is needed.
	MovedBaseSynced bool `json:"moved_base_synced,omitempty"`
	// MovedBaseLocalHash is the moved node's last-synced local content hash captured alongside
	// MovedBaseSynced (empty for a directory or a synced file baseline with no recorded hash).
	// Meaningful only while MovedBaseSynced is set: a non-synced pre-batch record's LocalHash
	// describes unpushed/divergent on-disk content, never a trustworthy base.
	MovedBaseLocalHash string `json:"moved_base_local_hash,omitempty"`
	// MovedBaseIsDir records that the moved node's reconcile-time baseline (the one
	// MovedBaseSynced describes) was a DIRECTORY, captured from the pre-batch BASELINE - not
	// from the move event's IsDir, which a sparse event reports as false regardless of the
	// node's real kind. It is the inherited mirror of BaseIsDir: a cross-batch delete that
	// inherits its base from this queued rename needs the base's kind, or a regular file that
	// locally replaced the tracked directory would fall into the "synced without hash: server
	// authority" branch and be destroyed with no _Conflict copy (closing
	// the sparse-dir-move bypass of the directory-kind gate). Meaningful only while
	// MovedBaseSynced is set. Like the sibling capture fields, additive with a tolerant
	// decode on both sides, so no CurrentSchemaVersion bump is needed.
	MovedBaseIsDir bool `json:"moved_base_is_dir,omitempty"`
}

// QueuedOp pairs an Op with the sequence number under which it is stored. The seq
// is the queue's stable identifier: Ack removes by it, and ascending seq is FIFO
// order.
type QueuedOp struct {
	Seq uint64
	Op
}

// Enqueue appends op to the durable queue and returns the monotonic sequence
// number assigned to it. NextSequence guarantees a strictly increasing key, so
// records iterate and drain in insertion (FIFO) order.
// enqueueTx appends one already-encoded op to the queue within the caller's write
// transaction, returning its sequence. It is the shared core of Enqueue and Batch.Enqueue,
// so a batched op is numbered from the same sequence and lands in the same bucket as a
// standalone one - the queue's order is the only thing the executor's serialisation reads.
func enqueueTx(tx *bolt.Tx, enc []byte) (uint64, error) {
	q := tx.Bucket(bucketQueue)
	seq, err := q.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("store: queue next sequence: %w", err)
	}
	if err := q.Put(encodeUint64(seq), enc); err != nil {
		return 0, fmt.Errorf("store: enqueue %d: %w", seq, err)
	}
	return seq, nil
}

// Enqueue appends one op to the durable queue in its own transaction, returning the sequence
// the executor will read it in. Batch.Enqueue is the same write with many ops sharing one
// transaction; both draw from the same sequence, so the two interleave in one order.
func (s *Store) Enqueue(op Op) (uint64, error) {
	enc, err := json.Marshal(op)
	if err != nil {
		return 0, fmt.Errorf("store: encode op: %w", err)
	}
	var seq uint64
	err = s.update(func(tx *bolt.Tx) error {
		var terr error
		seq, terr = enqueueTx(tx, enc)
		return terr
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// The queue is single-consumer: exactly one dispatcher drains it (the reconcile
// engine, DESIGN §6), reading the head with the bounded readers below, performing
// the side effect, then Acking the seq so a crash mid-op leaves the record in place
// to retry on restart. The worker pool parallelises the network transfers the
// dispatcher hands out; it does not drain the queue concurrently. None of the
// readers claim an op (there is no in-flight/lease marker), which is why competing
// consumers would need claim semantics (e.g. an in-flight bucket) added first.

// Ack removes the queued operation with the given sequence number. Acking an
// absent seq is a no-op, matching bbolt's Delete semantics; this lets the
// dispatcher confirm a processed op without racing its own concurrent drain.
func (s *Store) Ack(seq uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketQueue).Delete(encodeUint64(seq)); err != nil {
			return fmt.Errorf("store: ack %d: %w", seq, err)
		}
		return nil
	})
}

// PendingLen returns the number of operations currently queued.
func (s *Store) PendingLen() (int, error) {
	var n int
	err := s.view(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketQueue).Stats().KeyN
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// PendingOps returns every queued operation in ascending sequence (FIFO) order.
// It is a test-only convenience over the unbounded PendingOpsN(0); production
// drains in bounded batches via PendingOpsN / ScanPending instead, so the whole
// backlog is never materialised at once.
func (s *Store) PendingOps() ([]QueuedOp, error) {
	return s.PendingOpsN(0)
}

// ScanPending streams every queued operation to fn in ascending sequence (FIFO)
// order within a single read transaction, decoding one op at a time so a caller that
// must examine the whole queue can do so without materialising it all in memory. fn
// must not mutate the queue: the cursor is live inside the read transaction, so a
// caller that needs to drop ops should collect their seqs here and Ack them after
// ScanPending returns. A non-nil error from fn stops the scan and is returned; a
// record that fails to decode is skipped and quarantined (see ScanPendingFrom).
func (s *Store) ScanPending(fn func(QueuedOp) error) error {
	return s.ScanPendingFrom(0, fn)
}

// ScanPendingFrom streams queued operations to fn in ascending sequence (FIFO) order,
// starting at the first op whose seq is strictly greater than afterSeq (pass 0 to start
// from the beginning). It otherwise behaves like ScanPending: one op decoded at a time
// inside a read transaction, fn must not mutate the queue, and a non-nil error from fn
// stops the scan and is returned. A record that fails to decode (a corrupt value, or a
// malformed key) is skipped rather than failing the scan, and once the read transaction
// closes it is quarantined into the dead-letter bucket (see
// quarantineCorruptQueueRecords) - previously the first corrupt record
// aborted every read forever, wedging the whole sync pipeline until Reset. The exclusive
// start lets a caller page through a large queue - scan a bounded run, leave the read
// transaction to ack the ops it matched, then resume past the last op it saw - so the
// matched set never has to be held in memory all at once.
func (s *Store) ScanPendingFrom(afterSeq uint64, fn func(QueuedOp) error) error {
	var corrupt []corruptQueueRecord
	err := s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketQueue).Cursor()
		var k, raw []byte
		if afterSeq > 0 {
			// Seek lands on the first key >= afterSeq; advance past an exact match so
			// afterSeq is an exclusive lower bound.
			k, raw = c.Seek(encodeUint64(afterSeq))
			if k != nil && decodeUint64(k) == afterSeq {
				k, raw = c.Next()
			}
		} else {
			k, raw = c.First()
		}
		for ; k != nil; k, raw = c.Next() {
			qo, bad := decodeQueueRecord(k, raw)
			if bad != nil {
				corrupt = append(corrupt, *bad)
				continue
			}
			if err := fn(qo); err != nil {
				return err
			}
		}
		return nil
	})
	s.quarantineCorruptQueueRecords(corrupt)
	return err
}

// PendingOpsN returns up to limit queued operations in ascending sequence (FIFO)
// order, stopping the scan as soon as limit records have been read; a non-positive
// limit returns every queued operation. Bounding the read lets a caller that drains in
// fixed-size batches decode and hold only one batch at a time instead of re-decoding the
// whole backlog on every pass, so a large queue does not drive quadratic scanning or
// unbounded memory. A record that fails to decode is skipped (it does not count toward
// limit) and quarantined into the dead-letter bucket, mirroring ScanPendingFrom.
func (s *Store) PendingOpsN(limit int) ([]QueuedOp, error) {
	return s.PendingOpsNFrom(0, limit)
}

// PendingOpsNFrom behaves like PendingOpsN but starts at the first op whose seq is
// strictly greater than afterSeq (pass 0 for the queue head), mirroring
// ScanPendingFrom's exclusive lower bound. It lets the executor page its bounded
// drain window past ops an earlier window left queued - failed, skipped behind a
// failure, or absent-view-suppressed - instead of re-reading the same head batch
// every pass, which starved every op behind a head saturated with
// permanently-failing ops forever.
func (s *Store) PendingOpsNFrom(afterSeq uint64, limit int) ([]QueuedOp, error) {
	var out []QueuedOp
	var corrupt []corruptQueueRecord
	err := s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketQueue).Cursor()
		var k, raw []byte
		if afterSeq > 0 {
			// Seek lands on the first key >= afterSeq; advance past an exact match so
			// afterSeq is an exclusive lower bound (mirroring ScanPendingFrom).
			k, raw = c.Seek(encodeUint64(afterSeq))
			if k != nil && decodeUint64(k) == afterSeq {
				k, raw = c.Next()
			}
		} else {
			k, raw = c.First()
		}
		for ; k != nil; k, raw = c.Next() {
			if limit > 0 && len(out) >= limit {
				break
			}
			qo, bad := decodeQueueRecord(k, raw)
			if bad != nil {
				corrupt = append(corrupt, *bad)
				continue
			}
			out = append(out, qo)
		}
		return nil
	})
	s.quarantineCorruptQueueRecords(corrupt)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// corruptQueueRecord identifies a queue record whose key or value failed to decode
// during a read, for quarantineCorruptQueueRecords to move out of the queue once the
// read transaction has closed.
type corruptQueueRecord struct {
	// key is a copy of the raw bucket key (bbolt key slices are only valid inside
	// the transaction that produced them).
	key []byte
	// reason is the human-readable dead-letter reason, carrying the raw seq (or key
	// length) since the record's view/op fields are unknowable.
	reason string
}

// decodeQueueRecord decodes one queue bucket record. A malformed key (bbolt checksums
// only its meta pages, so latent file corruption can surface a key of any length) or an
// undecodable value yields a corruptQueueRecord instead of an error: previously
// the readers aborted the whole scan on the first bad record, turning one
// corrupt value into a global poison pill - every drain, queued-path lookup and
// supersession scan failed forever, with no way to learn the seq and Ack it away short
// of Reset/re-pair.
func decodeQueueRecord(k, raw []byte) (QueuedOp, *corruptQueueRecord) {
	seq, ok := decodeUint64OK(k)
	if !ok {
		return QueuedOp{}, &corruptQueueRecord{
			key:    append([]byte(nil), k...),
			reason: fmt.Sprintf("corrupt queue key (%d bytes)", len(k)),
		}
	}
	var op Op
	if err := json.Unmarshal(raw, &op); err != nil {
		return QueuedOp{}, &corruptQueueRecord{
			key:    append([]byte(nil), k...),
			reason: fmt.Sprintf("corrupt queue record (seq %d): %v", seq, err),
		}
	}
	return QueuedOp{Seq: seq, Op: op}, nil
}

// quarantineCorruptQueueRecords moves records a reader skipped as corrupt out of the
// queue and into the dead-letter bucket, so they leave the queue durably and surface as
// the `status` Dropped count instead of being silently re-skipped on every read. Each
// record's trace and queue delete run in one Update transaction (mirroring
// RecordDeadLetterAndAck) with a still-queued recheck first, so a replayed or concurrent
// read can never double-count Dropped. It is best-effort by design: the read that found
// the corruption has already succeeded, so a failure here is logged (the record stays
// queued, skipped again next read) rather than returned. The Warn goes to the store's
// logger (SetLogger - the daemon's rotating file, else slog.Default()); the durable
// dead-letter record itself is the primary observable.
func (s *Store) quarantineCorruptQueueRecords(corrupt []corruptQueueRecord) {
	for _, cq := range corrupt {
		enc, err := json.Marshal(DeadLetter{
			Reason:     cq.reason,
			RecordedAt: time.Now().Unix(),
		})
		if err != nil {
			s.log().Warn("store: encode corrupt-queue dead-letter", "reason", cq.reason, "error", err)
			continue
		}
		var dropped bool
		err = s.update(func(tx *bolt.Tx) error {
			q := tx.Bucket(bucketQueue)
			if q.Get(cq.key) == nil {
				// Already quarantined by an earlier read.
				return nil
			}
			dropped = true
			if _, err := recordDeadLetterTx(tx, enc); err != nil {
				return err
			}
			if err := q.Delete(cq.key); err != nil {
				return fmt.Errorf("store: drop corrupt queue record: %w", err)
			}
			return nil
		})
		if err != nil {
			s.log().Warn("store: quarantine corrupt queue record", "reason", cq.reason, "error", err)
			continue
		}
		if !dropped {
			// A concurrent or replayed read already quarantined this record: no trace was
			// written, so it must not be Warned or counted a second time.
			continue
		}
		// Count the drop before the Warn: the counter is what arms the engine's repair
		// sweep for the pending baseline this record's op was going to resolve
		// (QuarantinedOps, engine/queuerepair.go). Only a drop that actually committed is
		// counted, so one record can never arm the sweep twice.
		s.quarantined.Add(1)
		s.log().Warn("store: corrupt queue record dead-lettered", "reason", cq.reason)
	}
}

// HasQueuedOps reports whether the durable queue holds at least one record.
//
// It answers the second half of "is there work tied to the current on-disk
// layout?". Baselines are the usual evidence, but the enqueue-before-baseline
// doctrine means an op is durably queued BEFORE its pending baseline is written,
// so a crash in that window leaves a queue with no baselines behind it - and a
// relayout would then recompute that op's serialisation keys under the new
// layout. Cheap regardless of queue depth: it stops at the first key.
func (s *Store) HasQueuedOps() (bool, error) {
	var any bool
	err := s.view(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(bucketQueue).Cursor().First()
		any = k != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("store: check for queued ops: %w", err)
	}
	return any, nil
}
