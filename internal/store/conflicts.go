package store

import (
	"bytes"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Conflict is the durable record of one resolved keep-both conflict (DESIGN §6):
// the server version won on CanonicalPath and the divergent local content was
// preserved as a _Conflict copy at CopyPath, pushed back to the server under
// CopyFileID. It is keyed in the conflicts bucket by view_id\0canonical_path so the
// `conflicts` CLI subcommand can list outstanding conflicts per view; resolving the
// same canonical path again overwrites the record.
type Conflict struct {
	// CanonicalPath is the view-relative path the server version now occupies; it
	// is also the key suffix.
	CanonicalPath string `json:"canonical_path"`
	// CopyPath is the view-relative path of the preserved local _Conflict copy.
	CopyPath string `json:"copy_path"`
	// CopyFileID is the server node id assigned to the uploaded copy.
	CopyFileID string `json:"copy_file_id"`
	// DetectedAt is the unix time (seconds) the conflict was resolved.
	DetectedAt int64 `json:"detected_at"`
}

// conflictKey builds the conflicts-bucket key view_id\0canonical_path, matching the
// files-bucket key scheme (see baselineKey).
func conflictKey(viewID uint64, canonicalPath string) []byte {
	prefix := viewPrefix(viewID)
	key := make([]byte, 0, len(prefix)+len(canonicalPath))
	key = append(key, prefix...)
	key = append(key, canonicalPath...)
	return key
}

// PutConflict writes (creating or overwriting) the conflict record for
// view_id\0c.CanonicalPath. The value is JSON.
func (s *Store) PutConflict(viewID uint64, c Conflict) error {
	enc, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("store: encode conflict %d/%q: %w", viewID, c.CanonicalPath, err)
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketConflicts).Put(conflictKey(viewID, c.CanonicalPath), enc); err != nil {
			return fmt.Errorf("store: write conflict %d/%q: %w", viewID, c.CanonicalPath, err)
		}
		return nil
	})
}

// GetConflict returns the conflict record for view_id\0canonicalPath and whether one
// exists. A record whose value no longer decodes (bit rot) is reported as ABSENT with a
// Warn rather than as an error, matching ScanConflicts' skip-and-warn policy for this
// bucket: its only caller is the keep-both rollback below, which uses the result to
// restore what it displaced - surfacing the decode error there would turn one bit-rotted
// record into a permanently failing resolution, while treating it as absent lets the
// rollback delete it (the bucket's only in-band cure).
func (s *Store) GetConflict(viewID uint64, canonicalPath string) (Conflict, bool, error) {
	var rec Conflict
	var found bool
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketConflicts).Get(conflictKey(viewID, canonicalPath))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &rec); err != nil {
			s.log().Warn("store: ignoring corrupt conflict record", "view", viewID, "path", canonicalPath, "error", err)
			rec = Conflict{}
			return nil
		}
		found = true
		return nil
	})
	if err != nil {
		return Conflict{}, false, fmt.Errorf("store: read conflict %d/%q: %w", viewID, canonicalPath, err)
	}
	return rec, found, nil
}

// DeleteConflict removes the conflict record for view_id\0canonicalPath. Deleting an
// absent record is a no-op, matching bbolt's Delete semantics. Production only ever
// deletes to roll back a keep-both provenance row it wrote speculatively but whose
// _Conflict copy the filesystem then refused (engine's preserveAndRecord) - a resolved
// conflict's record is retired by overwriting it, never by a bare delete.
func (s *Store) DeleteConflict(viewID uint64, canonicalPath string) error {
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketConflicts).Delete(conflictKey(viewID, canonicalPath)); err != nil {
			return fmt.Errorf("store: delete conflict %d/%q: %w", viewID, canonicalPath, err)
		}
		return nil
	})
}

// ScanConflicts streams every conflict record in view to fn in ascending
// canonical-path order (bbolt's byte order over the path suffix) within a single
// read transaction, decoding one record at a time so a caller examining the whole
// view - the daemon's buildConflicts newest-N selection, buildStatus's count -
// need not materialise the bucket. It mirrors ScanBaselinesForView/ScanPending:
// fn must not mutate the conflicts bucket (the cursor is live), and a non-nil
// error from fn stops the scan and is returned. A record whose value no longer
// decodes (bit rot - bbolt checksums only its meta pages) is skipped with a Warn
// rather than aborting the whole scan: it backs exactly the two verbs an operator
// would diagnose corruption with (`status`/`conflicts`), so surfacing the decode
// error would make them fail forever on one bit-rotted record (no operator-facing
// cure - only a same-view/path overwrite, the keep-both rollback's DeleteConflict
// or Reset clears one). The
// healthy siblings still stream, the recoverable record stays on disk for
// inspection (like ListViews), and the path suffix recovers
// view+path for the Warn. The prefix cursor never touches records of other views.
func (s *Store) ScanConflicts(viewID uint64, fn func(Conflict) error) error {
	prefix := viewPrefix(viewID)
	return s.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketConflicts).Cursor()
		for k, raw := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, raw = c.Next() {
			var rec Conflict
			if err := json.Unmarshal(raw, &rec); err != nil {
				path := string(k[len(prefix):])
				s.log().Warn("store: skipping corrupt conflict record", "view", viewID, "path", path, "error", err)
				continue
			}
			if err := fn(rec); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListConflicts returns every conflict record in view, ordered by ascending
// canonical path, materialising the whole view via ScanConflicts. The daemon's
// status/conflicts paths stream via ScanConflicts instead so a mass-divergence
// bucket is never held whole in memory; this slice form is
// kept for tests and any caller that wants the whole view at once.
func (s *Store) ListConflicts(viewID uint64) ([]Conflict, error) {
	var out []Conflict
	err := s.ScanConflicts(viewID, func(rec Conflict) error {
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
