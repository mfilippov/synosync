package store

import (
	"bytes"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// keySep separates the big-endian uint64 view_id from the path (or file_id) in
// the composite keys of the files and byid buckets. A NUL byte cannot appear in
// a view-relative path, so it is an unambiguous delimiter that also yields a
// clean per-view prefix (view_id + NUL) for cursor iteration.
const keySep = 0x00

// BaselineState is the reconcile state recorded for a baseline record. It mirrors
// where the file sits in the sync lifecycle (DESIGN §6); the sync engine sets and
// consumes it.
type BaselineState string

const (
	// StateSynced means local and server agree at the recorded hashes/sync id.
	//
	// Invariant (DESIGN §6): only the APPLY path advances a record to StateSynced,
	// never the reconcile pass. Reconcile marks a detected change StatePending (or
	// StateConflict) and lets the queued op reach synced when it applies; the sole
	// reconcile-side writer of synced is the self-echo path (our own committed change),
	// which advances only a record NOT already owned by a queued apply - an existing
	// StatePending/StateConflict is preserved (overlaySelfEchoFacts).
	// supersededReplay in inbound_rename.go leans on this: a synced destination record
	// is its proof "a prior apply already ran", so a reconcile-side synced stamp over a
	// relocated pending record would forge that proof and skip a real on-disk move.
	StateSynced BaselineState = "synced"
	// StatePending means a local or remote change has been detected but not yet
	// reconciled.
	StatePending BaselineState = "pending"
	// StateConflict means local and server diverged from the baseline at once and
	// a keep-both resolution is owed.
	StateConflict BaselineState = "conflict"
)

// Baseline is the stored snapshot of one file or directory as of the last
// successful sync, keyed in the files bucket by view_id\0path (DESIGN §5). It
// carries both the server-side and last-known-local facts the reconcile detector
// compares against. A matching byid index entry (view_id\0file_id → path) is
// maintained atomically alongside it.
type Baseline struct {
	// FileID is the server's opaque node identifier; it is the byid index value's
	// counterpart and survives renames. It is empty for a local-only baseline that
	// the server has not yet assigned an id to; such records are not byid-indexed.
	FileID string `json:"file_id"`
	// FileType is the node kind as the server reports it (e.g. "file"/"dir").
	FileType string `json:"file_type"`
	// ServerHash is the server-reported MD4 content hash (lowercase hex).
	ServerHash string `json:"server_hash"`
	// ServerMTime is the server's modification timestamp.
	ServerMTime int64 `json:"server_mtime"`
	// ServerSyncID is the sync id at which this server state was observed.
	ServerSyncID int64 `json:"server_sync_id"`
	// LocalHash is the last-known local MD4 content hash (lowercase hex). A StateSynced FILE
	// record normally carries a non-empty value - the engine's markSynced always derives one -
	// so a StateSynced file record with an empty LocalHash is anomalous (produced by the
	// inbound-rename promoted-pending write, which relocates a file onto a path and cannot
	// know its hash; the BARE write of that pair no longer reaches StateSynced at all, since a
	// record naming a node with neither hash nor tracking evidence is recorded StateConflict -
	// claiming agreement there let the next scan push unattributable bytes over the server's
	// node). The engine's never-lose gates treat that
	// anomaly as "no trustworthy divergence base → preserve whole" rather than "in sync → server
	// authority", so user bytes are never silently clobbered/deleted on a guess (see
	// removeBaseFor / relocatedRemoveBase / untrackedFilePresent / guardRenameDest). A StateSynced
	// DIRECTORY record's empty LocalHash is structural and legitimate.
	LocalHash string `json:"local_hash"`
	// LocalMTime is the last-known local modification timestamp.
	LocalMTime int64 `json:"local_mtime"`
	// LocalSize is the last-known local content length in bytes.
	LocalSize int64 `json:"local_size"`
	// UnixPerm is the last-known local permission bits.
	UnixPerm uint32 `json:"unix_perm"`
	// State is the reconcile lifecycle state of this record.
	State BaselineState `json:"state"`
}

// baselineKey builds the files-bucket key view_id\0path.
func baselineKey(viewID uint64, path string) []byte {
	prefix := viewPrefix(viewID)
	key := make([]byte, 0, len(prefix)+len(path))
	key = append(key, prefix...)
	key = append(key, path...)
	return key
}

// byIDKey builds the byid-bucket key view_id\0file_id.
func byIDKey(viewID uint64, fileID string) []byte {
	prefix := viewPrefix(viewID)
	key := make([]byte, 0, len(prefix)+len(fileID))
	key = append(key, prefix...)
	key = append(key, fileID...)
	return key
}

// viewPrefix is the 9-byte big-endian view_id followed by the NUL separator; it
// is the cursor prefix for scanning every record of one view.
func viewPrefix(viewID uint64) []byte {
	prefix := make([]byte, 0, 9)
	prefix = append(prefix, encodeUint64(viewID)...)
	prefix = append(prefix, keySep)
	return prefix
}

// PutBaseline writes (creating or overwriting) the baseline for view_id\0path and
// keeps the byid index consistent, both within one write transaction so the index
// can never drift from the baseline. If a record already existed at this path
// under a different file_id, its stale byid entry is removed; if the incoming
// file_id was already indexed at a different path (a move expressed as a single
// Put at the new path), the orphan baseline at the old path is removed too.
func (s *Store) PutBaseline(viewID uint64, path string, b Baseline) error {
	enc, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("store: encode baseline %d/%q: %w", viewID, path, err)
	}
	return s.update(func(tx *bolt.Tx) error {
		return putBaselineTx(tx, viewID, path, enc, b.FileID)
	})
}

// putBaselineTx writes the already-encoded baseline at view_id\0path and keeps the
// byid index consistent, within the caller's write transaction. It is the shared
// core of PutBaseline and MoveBaseline's destination write, so both share one
// definition of the index guards. fileID is the incoming record's FileID: only
// records that carry a server file_id are indexed - an empty file_id is not a real
// identity (it marks a local-only baseline awaiting a server id), so it must not
// enter byid: the bare key would collide across every unindexed path and the
// move-detection below would treat unrelated records as moves of one another,
// deleting live baselines.
func putBaselineTx(tx *bolt.Tx, viewID uint64, path string, enc []byte, fileID string) error {
	files := tx.Bucket(bucketFiles)
	byid := tx.Bucket(bucketByID)
	key := baselineKey(viewID, path)

	// If the path is being re-pointed at a different file_id, drop the old
	// index entry so byid never references a vanished file_id. A prior record
	// with no file_id (a local-only baseline not yet assigned a server id) was
	// never indexed, so there is nothing to drop for it. A prior that no longer
	// decodes (on-disk corruption) is treated as absent rather than surfaced as an
	// error: failing here would make the overwrite - the one operation that can
	// heal the record - the one operation that can never run, wedging the key (and
	// the view's inbound cursor behind it) until Reset/re-pair, the same
	// poison-pill shape DeleteBaseline already sheds. Its file_id being unrecoverable from the corrupt bytes, the view's
	// byid entries are swept by value == path instead, so the index never keeps a
	// dangling reference to the record being overwritten.
	if old := files.Get(key); old != nil {
		var prev Baseline
		if err := json.Unmarshal(old, &prev); err != nil {
			if serr := sweepByIDPath(byid, viewID, path); serr != nil {
				return serr
			}
		} else if prev.FileID != "" && prev.FileID != fileID {
			if err := byid.Delete(byIDKey(viewID, prev.FileID)); err != nil {
				return fmt.Errorf("store: drop stale index %d/%q: %w", viewID, prev.FileID, err)
			}
		}
	}

	if fileID != "" {
		// If this file_id is already indexed at a different path - a move/rename
		// expressed as a single Put at the new path - drop the orphan baseline at
		// the old path so two files records can never share one file_id.
		if oldPath := byid.Get(byIDKey(viewID, fileID)); oldPath != nil && !bytes.Equal(oldPath, []byte(path)) {
			if err := files.Delete(baselineKey(viewID, string(oldPath))); err != nil {
				return fmt.Errorf("store: drop moved baseline %d/%q: %w", viewID, oldPath, err)
			}
		}
	}

	if err := files.Put(key, enc); err != nil {
		return fmt.Errorf("store: write baseline %d/%q: %w", viewID, path, err)
	}
	if fileID != "" {
		if err := byid.Put(byIDKey(viewID, fileID), []byte(path)); err != nil {
			return fmt.Errorf("store: write index %d/%q: %w", viewID, fileID, err)
		}
	}
	return nil
}

// MoveBaseline atomically relocates a baseline from fromPath to toPath: it writes b at
// toPath (maintaining the byid index exactly as PutBaseline does) and removes whatever
// record is left at fromPath, all in a single write transaction. For an indexed node
// (non-empty file_id) PutBaseline's byid-driven move already drops the old path, so a plain
// Put would suffice; but a local-only record (empty file_id) is unindexed and needs an
// explicit delete of fromPath. Doing the write and that delete in one transaction is what
// makes a directory-subtree relocation crash-replay safe: a crash between a separate Put and
// Delete would leave a duplicate at fromPath that the relocation's destination-occupied guard
// then skips forever, stranding it. fromPath == toPath collapses to a plain PutBaseline.
func (s *Store) MoveBaseline(viewID uint64, fromPath, toPath string, b Baseline) error {
	if fromPath == toPath {
		return s.PutBaseline(viewID, toPath, b)
	}
	enc, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("store: encode baseline %d/%q: %w", viewID, toPath, err)
	}
	return s.update(func(tx *bolt.Tx) error {
		return moveBaselineTx(tx, viewID, fromPath, toPath, enc, b.FileID)
	})
}

// moveBaselineTx is MoveBaseline's transaction body, shared with Batch so a batched
// relocation gets the same index guards - and the same corrupt-source sweep - as a
// standalone one.
func moveBaselineTx(tx *bolt.Tx, viewID uint64, fromPath, toPath string, enc []byte, fileID string) error {
	// The destination write shares putBaselineTx's index guards: re-pointing
	// toPath at a different file_id drops the old index entry, and an indexed
	// file_id already recorded at another path has its orphan baseline dropped.
	// When that other path is fromPath - the common relocate case - the guard
	// already removes the source record, leaving the explicit fromPath cleanup
	// below a no-op.
	if err := putBaselineTx(tx, viewID, toPath, enc, fileID); err != nil {
		return err
	}
	files := tx.Bucket(bucketFiles)
	byid := tx.Bucket(bucketByID)
	// Remove whatever record still sits at fromPath. For an indexed node the byid move
	// above already deleted it (this is then a no-op); for a local-only node this is the
	// only delete. Drop its byid entry only if it still points at fromPath - never the
	// entry the Put above just re-pointed at toPath. A record that no longer decodes is
	// deleted anyway (mirroring DeleteBaseline - requiring a clean
	// decode would wedge every relocation over the corrupt source forever); its file_id
	// being unrecoverable, the view's byid entries still pointing at fromPath are swept
	// instead.
	fromKey := baselineKey(viewID, fromPath)
	if raw := files.Get(fromKey); raw != nil {
		var prev Baseline
		decodeErr := json.Unmarshal(raw, &prev)
		if err := files.Delete(fromKey); err != nil {
			return fmt.Errorf("store: delete moved-from baseline %d/%q: %w", viewID, fromPath, err)
		}
		if decodeErr != nil {
			return sweepByIDPath(byid, viewID, fromPath)
		}
		if prev.FileID != "" {
			if cur := byid.Get(byIDKey(viewID, prev.FileID)); bytes.Equal(cur, []byte(fromPath)) {
				if err := byid.Delete(byIDKey(viewID, prev.FileID)); err != nil {
					return fmt.Errorf("store: delete moved-from index %d/%q: %w", viewID, prev.FileID, err)
				}
			}
		}
	}
	return nil
}

// GetBaseline returns the baseline for view_id\0path, or ErrNotFound if absent.
// getBaselineTx is GetBaseline's read within the caller's transaction. It is shared with
// Batch so a batched apply reads a record exactly as a standalone one does - including the
// ErrCorruptRecord tagging its callers branch on.
func getBaselineTx(tx *bolt.Tx, viewID uint64, path string) (Baseline, error) {
	raw := tx.Bucket(bucketFiles).Get(baselineKey(viewID, path))
	if raw == nil {
		return Baseline{}, fmt.Errorf("%w: files/%d/%q", ErrNotFound, viewID, path)
	}
	var b Baseline
	if err := json.Unmarshal(raw, &b); err != nil {
		// Tag the decode failure with ErrCorruptRecord (in addition to the underlying
		// json error) so a caller resolving this record - resolveParentID chief among
		// them - can tell an unrecoverable, will-never-decode value apart from a
		// transient backend error and dead-letter/freeze the dependent op rather than
		// retrying it against the corrupt parent forever.
		return Baseline{}, fmt.Errorf("store: decode baseline %d/%q: %w: %w", viewID, path, ErrCorruptRecord, err)
	}
	return b, nil
}

// GetBaseline returns the baseline for view_id\0path, or ErrNotFound if absent. A record
// that no longer decodes is reported with ErrCorruptRecord so a caller can tell an
// unrecoverable value apart from a transient backend error.
func (s *Store) GetBaseline(viewID uint64, path string) (Baseline, error) {
	var b Baseline
	err := s.view(func(tx *bolt.Tx) error {
		var gerr error
		b, gerr = getBaselineTx(tx, viewID, path)
		return gerr
	})
	if err != nil {
		return Baseline{}, err
	}
	return b, nil
}

// DeleteBaseline removes the baseline for view_id\0path together with its byid
// index entry, atomically. Deleting an absent baseline is a no-op. A record that
// no longer decodes (on-disk corruption) is deleted anyway rather than surfaced as
// an error: requiring a clean decode would wedge the key forever - every delete of
// it re-fails, only a full Reset/re-pair cures it - turning one bad value into a
// permanent poison pill. Its file_id is unrecoverable from the
// corrupt bytes, so the view's byid entries are swept for any that still point at
// this path, keeping the index free of dangling references.
func (s *Store) DeleteBaseline(viewID uint64, path string) error {
	return s.update(func(tx *bolt.Tx) error {
		return deleteBaselineTx(tx, viewID, path)
	})
}

// deleteBaselineTx is DeleteBaseline's body, factored out so the batched writer can drop many
// baselines in ONE transaction (see Batch.DeleteBaseline). The semantics - including the
// corrupt-record sweep - are the single implementation both granularities run.
func deleteBaselineTx(tx *bolt.Tx, viewID uint64, path string) error {
	files := tx.Bucket(bucketFiles)
	key := baselineKey(viewID, path)
	raw := files.Get(key)
	if raw == nil {
		return nil
	}
	var b Baseline
	decodeErr := json.Unmarshal(raw, &b)
	if err := files.Delete(key); err != nil {
		return fmt.Errorf("store: delete baseline %d/%q: %w", viewID, path, err)
	}
	byid := tx.Bucket(bucketByID)
	if decodeErr != nil {
		// Corrupt record: its file_id is unknowable, so sweep the view's index
		// for entries still pointing at this path.
		return sweepByIDPath(byid, viewID, path)
	}
	// Only records with a server file_id were indexed (see putBaselineTx), so a
	// local-only baseline has no byid entry to remove.
	if b.FileID != "" {
		if err := byid.Delete(byIDKey(viewID, b.FileID)); err != nil {
			return fmt.Errorf("store: delete index %d/%q: %w", viewID, b.FileID, err)
		}
	}
	return nil
}

// sweepByIDPath removes every byid entry of the view that still points at path. It is
// the recovery core for a baseline record whose value no longer decodes (on-disk
// corruption): the record's file_id is unknowable from the corrupt bytes, so the
// view's index is swept by value == path instead, keeping it free of dangling
// references. Collect first, then delete - deleting under a live bbolt cursor skips
// the element after each removal. Shared by DeleteBaseline's corrupt branch,
// putBaselineTx's corrupt-prior overwrite and MoveBaseline's corrupt moved-from
// cleanup, so one definition owns the sweep.
func sweepByIDPath(byid *bolt.Bucket, viewID uint64, path string) error {
	prefix := viewPrefix(viewID)
	var doomed [][]byte
	c := byid.Cursor()
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		if string(v) == path {
			doomed = append(doomed, append([]byte(nil), k...))
		}
	}
	for _, k := range doomed {
		if err := byid.Delete(k); err != nil {
			return fmt.Errorf("store: delete index %d/%q: %w", viewID, k[len(prefix):], err)
		}
	}
	return nil
}

// PathByID returns the view-relative path currently indexed for file_id in view,
// or ErrNotFound if no baseline references it. The sync engine uses it to resolve
// move/rename events that arrive keyed by file_id.
func (s *Store) PathByID(viewID uint64, fileID string) (string, error) {
	var path string
	err := s.view(func(tx *bolt.Tx) error {
		var terr error
		path, terr = pathByIDTx(tx, viewID, fileID)
		return terr
	})
	if err != nil {
		return "", err
	}
	return path, nil
}

// pathByIDTx is the byid lookup inside a caller's transaction, so a Batch can resolve a node
// in the same transaction as the writes that depend on the answer.
func pathByIDTx(tx *bolt.Tx, viewID uint64, fileID string) (string, error) {
	raw := tx.Bucket(bucketByID).Get(byIDKey(viewID, fileID))
	if raw == nil {
		return "", fmt.Errorf("%w: byid/%d/%q", ErrNotFound, viewID, fileID)
	}
	return string(raw), nil
}

// PathBaseline pairs a baseline record with its view-relative path for
// BaselinesForView.
type PathBaseline struct {
	Path string
	Baseline
}

// ScanBaselinesForView streams every baseline in view to fn in ascending path
// order (bbolt's byte order over the path suffix) within a single read
// transaction, decoding one baseline at a time so a caller that must examine the
// whole view can do so without materialising it all in memory. It mirrors the
// queue's ScanPending: fn must not mutate the files bucket (the cursor is live
// inside the read transaction), and a non-nil error from fn stops the scan and is
// returned. A record whose value no longer decodes (on-disk corruption - bbolt
// checksums only its meta pages) is skipped rather than failing the scan, and its
// view-relative path (still recoverable from the key) is returned in skipped so
// callers can Warn and leave the path alone - previously the first
// corrupt record aborted every scan, killing the whole view's outbound reconcile
// (and the status unresolved count) until Reset/re-pair. skipped carries whatever
// was collected even when fn stops the scan early, so sentinel-based early-exit
// callers still see the corrupt paths encountered so far. The prefix cursor never
// touches records of other views.
func (s *Store) ScanBaselinesForView(viewID uint64, fn func(PathBaseline) error) (skipped []string, err error) {
	prefix := viewPrefix(viewID)
	err = s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketFiles).Cursor()
		for k, raw := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, raw = c.Next() {
			var b Baseline
			if err := json.Unmarshal(raw, &b); err != nil {
				skipped = append(skipped, string(k[len(prefix):]))
				continue
			}
			if err := fn(PathBaseline{Path: string(k[len(prefix):]), Baseline: b}); err != nil {
				return err
			}
		}
		return nil
	})
	return skipped, err
}

// ScanBaselinesUnderPrefix streams the baselines of view recorded strictly beneath dir -
// the paths beginning with dir + "/" - to fn in ascending path order within a single read
// transaction. It is ScanBaselinesForView narrowed by a second cursor prefix: the composite
// key is view_id\0path, so one subtree's records are contiguous in bbolt's byte order and the
// scan can Seek straight to them and stop at the first key past the subtree. A directory
// relocation or subtree drop therefore decodes only the subtree's records rather than the
// whole view's - those helpers sit on the inbound apply path and used to
// pay O(view) JSON decodes per directory op.
//
// The remaining semantics mirror ScanBaselinesForView: fn must not mutate the files bucket
// (the cursor is live inside the read transaction, so the engine's mutating callers collect
// first and write after), a non-nil error from fn stops the scan and is returned, and a record
// whose value no longer decodes is skipped with its view-relative path returned in skipped.
// Only paths under the prefix can appear in skipped, so callers need not re-filter it. The
// record of dir ITSELF is deliberately not included: its own baseline is owned by the apply
// that moves or deletes the directory, not by the subtree sweep.
func (s *Store) ScanBaselinesUnderPrefix(viewID uint64, dir string, fn func(PathBaseline) error) (skipped []string, err error) {
	vp := viewPrefix(viewID)
	prefix := make([]byte, 0, len(vp)+len(dir)+1)
	prefix = append(prefix, vp...)
	prefix = append(prefix, dir...)
	prefix = append(prefix, '/')
	err = s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketFiles).Cursor()
		for k, raw := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, raw = c.Next() {
			var b Baseline
			if err := json.Unmarshal(raw, &b); err != nil {
				skipped = append(skipped, string(k[len(vp):]))
				continue
			}
			if err := fn(PathBaseline{Path: string(k[len(vp):]), Baseline: b}); err != nil {
				return err
			}
		}
		return nil
	})
	return skipped, err
}

// HasBaselines reports whether the store holds at least one baseline, i.e.
// whether anything has been synced yet.
//
// It answers one question the CLI cannot ask any other way: is there a local
// tree that a layout change would relocate? Cheap regardless of tree size - it
// stops at the first key rather than counting.
func (s *Store) HasBaselines() (bool, error) {
	var any bool
	err := s.view(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(bucketFiles).Cursor().First()
		any = k != nil
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("store: check for baselines: %w", err)
	}
	return any, nil
}
