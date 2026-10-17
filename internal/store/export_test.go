package store

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// This file holds test-only Store helpers. Living in an _test.go file keeps them
// out of the shipped public API (they compile only under `go test`) while staying
// reachable from the store package's own tests. Production drops a view record
// through the refresh path, never through a bare delete, so this stayed test-only -
// moving it here narrows the public surface accordingly. (DeleteConflict lived here
// too until the keep-both rollback in engine's preserveAndRecord needed it in
// production; it now ships in conflicts.go.)

// DeleteView removes the view record for viewID. Deleting an absent view is a
// no-op, matching bbolt's Delete semantics.
func (s *Store) DeleteView(viewID uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketViews).Delete(encodeUint64(viewID)); err != nil {
			return fmt.Errorf("store: delete view %d: %w", viewID, err)
		}
		return nil
	})
}

// RecordDeadLetter used to live here as a test-only shim: every
// production trace was written atomically with a queue ack, so a bare append had no
// caller outside the ring/trim tests. It ships in deadletter.go now that the engine
// records a trace for a condition that never had a queued op to ack - two local names
// folding to one sync key, which the durable queue cannot even represent (finding H6).

// OpenWithBoltOptions opens a store with caller-supplied bbolt options, so a benchmark can
// compare the shipped configuration against an alternative WITHOUT the alternative existing in
// production code. Open() is the only production entry point and it hard-codes its options on
// purpose; a measurement that has not yet justified a change must not widen that.
//
// It skips Open's state-directory creation (the benchmark supplies its own path) but is
// otherwise the same sequence: open then bootstrap (which is where migration runs).
func OpenWithBoltOptions(path string, opts *bolt.Options) (*Store, error) {
	db, err := bolt.Open(path, 0o600, opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", path, err)
	}
	s := &Store{db: db}
	if err := s.bootstrap(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}
