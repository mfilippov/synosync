package store

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// View is the stored catalog record for one shared folder (Synology
// "team folder"/view) the account syncs. It is keyed in the views bucket by its
// view_id as a big-endian uint64, so ListViews iterates in numeric id order.
type View struct {
	// NodeID is the server's opaque node identifier for the view root, used to
	// address view-relative paths. It is the root's FILE id - the value an upload
	// addresses its parent by - which is a different identifier from RootNodeID.
	NodeID string `json:"node_id"`
	// RootNodeID is the view root's small integer node id, as reported by
	// query_user_info's view_list. It is NOT the file id in NodeID: the upload verb
	// carries it as root_node_id and rejects a wrong (or zero) value with 12291, and
	// the long_poll subscribe entry names the same identifier. An absent field
	// decodes as 0, which refreshViews re-resolves from the next view_list.
	RootNodeID int64 `json:"root_node_id,omitempty"`
	// Name is the human-readable view name.
	Name string `json:"name"`
	// Privilege is the access level the account holds on the view (e.g. "rw").
	Privilege string `json:"privilege"`
	// LastSeenSyncID is the highest server sync id observed for this view; the
	// sync engine advances it as it consumes the change cursor.
	LastSeenSyncID int64 `json:"last_seen_sync_id"`
	// InitialSyncDone reports whether the engine has completed the Phase-6e
	// cold-start full-catalog pull for this view. An absent field decodes as
	// false (cold-start not yet performed); the schema v2→v3 migration backfills
	// pre-existing views to true since they were already syncing incrementally.
	InitialSyncDone bool `json:"initial_sync_done,omitempty"`
	// RootDevice is the filesystem device id (Unix st_dev / Windows volume serial)
	// the view's on-disk root last presented while healthy (observed non-empty).
	// The engine persists it so the empty-scan mount-swap guard survives a daemon
	// restart: after a restart the in-memory device map is empty, and without a
	// persisted baseline a restart into an already-dropped/swapped mount (an
	// ordinary-looking empty directory) would bypass the guard and let consecutive
	// empty scans erase the remote view. An absent field decodes as 0 ("not yet
	// recorded"), which the guard treats as nothing-to-compare.
	RootDevice uint64 `json:"root_device,omitempty"`
	// Rescan lists view-relative subtrees the engine must walk and merge like a first sync
	// before it trusts the incremental poll for them: a path whose exclusion was lifted, or
	// one a server move brought in from an excluded path. The event log reads forward and a
	// directory move emits nothing for its children, so without the walk such a subtree
	// would stay missing. An entry is removed once its walk completes; a pending full cold
	// start covers them all.
	Rescan []string `json:"rescan,omitempty"`
}

// PutView writes (creating or overwriting) the view record for viewID. The key
// is the big-endian uint64 view_id and the value is JSON.
func (s *Store) PutView(viewID uint64, v View) error {
	enc, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("store: encode view %d: %w", viewID, err)
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketViews).Put(encodeUint64(viewID), enc); err != nil {
			return fmt.Errorf("store: write view %d: %w", viewID, err)
		}
		return nil
	})
}

// GetView returns the view record for viewID, or ErrNotFound if it was never
// written.
func (s *Store) GetView(viewID uint64) (View, error) {
	var v View
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketViews).Get(encodeUint64(viewID))
		if raw == nil {
			return fmt.Errorf("%w: views/%d", ErrNotFound, viewID)
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			// Tag the decode failure with ErrCorruptRecord (mirroring GetBaseline/GetConfig)
			// so a resolving caller - refreshViews chief among them - can tell an
			// unrecoverable, will-never-decode value apart from a transient backend error and
			// heal it via overwrite instead of aborting the whole view refresh forever
			// (the tolerant ListViews reader already skips such a record; the single-record
			// strict reader now surfaces it distinctly).
			return fmt.Errorf("store: decode view %d: %w: %w", viewID, ErrCorruptRecord, err)
		}
		return nil
	})
	if err != nil {
		return View{}, err
	}
	return v, nil
}

// ListView pairs a view record with its view_id for ListViews.
type ListView struct {
	ViewID uint64
	View
}

// ListViews returns every stored view ordered by ascending view_id.
func (s *Store) ListViews() ([]ListView, error) {
	var out []ListView
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketViews).ForEach(func(k, raw []byte) error {
			id, ok := decodeUint64OK(k)
			if !ok {
				// A malformed key is unreachable corruption: no real view id can
				// address it (PutView/GetView encode ids to exactly 8 bytes), so
				// skip it rather than panicking in decodeUint64 or surfacing a phantom view 0.
				return nil
			}
			var v View
			if err := json.Unmarshal(raw, &v); err != nil {
				// A corrupt view value must not abort the whole scan: ListViews
				// feeds localScan, the poll loop, cold start and status, so one
				// bit-rotted record would wedge the entire daemon until Reset -
				// the global poison-pill class already closed for
				// the queue and files buckets, left open here for views. Skip it
				// (leaving the recoverable record in place rather than deleting
				// it): the view's healthy siblings keep syncing, and the skipped
				// view fails toward unsynced - never scanned, so its subtree is
				// never diffed as vanished and erased - rather than global.
				s.log().Warn("store: skipping corrupt view record", "view", id, "error", err)
				return nil
			}
			out = append(out, ListView{ViewID: id, View: v})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
