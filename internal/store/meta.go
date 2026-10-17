package store

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// The non-secret identity values (device_uuid, ds_id) live inside the config
// blob (meta/config, see config.go); the pinned-cert fingerprint lives in the
// keychain. The meta bucket otherwise holds only schema_version (owned by the
// migration runner), the config blob, and the operator's persisted pause state
// (pausedKey).

// SchemaVersion returns the on-disk schema version recorded by the migration
// runner. It is always present after Open.
func (s *Store) SchemaVersion() (uint64, error) {
	var v uint64
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketMeta).Get([]byte(schemaVersionKey))
		if raw == nil {
			return fmt.Errorf("%w: meta/%s", ErrNotFound, schemaVersionKey)
		}
		if len(raw) != 8 {
			return fmt.Errorf("store: corrupt schema_version: %d bytes", len(raw))
		}
		v = decodeUint64(raw)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return v, nil
}

// pausedKey is the meta-bucket key holding the operator's desired pause state
// (one byte, 1 = paused), so an explicit `synosync pause` survives a daemon
// restart ("paused until explicit resume"). The key is additive: every reader
// of the meta bucket addresses its own key (schema_version, config), so an
// older binary opening a store carrying this extra key simply never sees it -
// no CurrentSchemaVersion bump is required.
const pausedKey = "paused"

// PutPaused records the operator's desired pause state.
func (s *Store) PutPaused(paused bool) error {
	v := []byte{0}
	if paused {
		v = []byte{1}
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Put([]byte(pausedKey), v); err != nil {
			return fmt.Errorf("store: write paused: %w", err)
		}
		return nil
	})
}

// GetPaused reads the persisted pause state. It decodes tolerantly: an absent
// key (a store from before the key existed, or a never-paused install) and any
// unrecognized value both read as false rather than an error, so a stray or
// corrupt value can never wedge daemon startup - the failure direction is
// "runs unpaused", the pre-persistence behaviour.
func (s *Store) GetPaused() (bool, error) {
	var paused bool
	err := s.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMeta).Get([]byte(pausedKey))
		paused = len(v) == 1 && v[0] == 1
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("store: read paused: %w", err)
	}
	return paused, nil
}

// lastStopKey holds the reason the daemon last stopped for a reason the operator has to act
// on. Additive and tolerantly decoded like pausedKey, so no CurrentSchemaVersion bump is
// required: an older binary opening a store carrying it simply never reads the key.
const lastStopKey = "last_stop"

// LastStop is the daemon's last terminal exit: what happened, and what to do about it.
type LastStop struct {
	// Reason is the operator-facing sentence, e.g. "the server no longer recognises this
	// session".
	Reason string `json:"reason"`
	// Hint is the command that resolves it, e.g. "synosync login". Empty when there is none.
	Hint string `json:"hint,omitempty"`
	// At is the unix time (seconds) the daemon stopped.
	At int64 `json:"at"`
}

// PutLastStop records why the daemon stopped, so a CLI run afterwards can say so.
//
// Under a service manager this is the only way the reason reaches anyone. The daemon prints
// its hint to stderr and exits; systemd, launchd or the SCM restarts it, it fails the same way
// and dies again, and `synosync status` - which needs a running daemon to say anything at all -
// reports only "daemon is not running". The actionable half (re-pair, re-login, update) was
// visible solely to whoever was watching the terminal, or willing to read a rotating log.
func (s *Store) PutLastStop(ls LastStop) error {
	blob, err := json.Marshal(ls)
	if err != nil {
		return fmt.Errorf("store: encode last stop: %w", err)
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Put([]byte(lastStopKey), blob); err != nil {
			return fmt.Errorf("store: write last stop: %w", err)
		}
		return nil
	})
}

// ClearLastStop drops the record. The daemon calls it once it is up, so a stale reason from a
// fault that has since been fixed is not reported to the next CLI run.
func (s *Store) ClearLastStop() error {
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Delete([]byte(lastStopKey)); err != nil {
			return fmt.Errorf("store: clear last stop: %w", err)
		}
		return nil
	})
}

// GetLastStop reads the recorded reason, reporting whether one is present.
//
// Tolerant in the same direction as GetPaused: an absent key and an undecodable value both
// read as "nothing recorded" rather than an error. This is a diagnostic aid consulted while
// explaining that the daemon is not running, so it must never itself become the reason a
// command fails.
func (s *Store) GetLastStop() (LastStop, bool, error) {
	var ls LastStop
	var found bool
	err := s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketMeta).Get([]byte(lastStopKey))
		if len(raw) == 0 {
			return nil
		}
		if err := json.Unmarshal(raw, &ls); err != nil {
			return nil
		}
		found = ls.Reason != ""
		return nil
	})
	if err != nil {
		return LastStop{}, false, fmt.Errorf("store: read last stop: %w", err)
	}
	return ls, found, nil
}
