package store

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/config"
)

// configKey is the meta-bucket key holding the non-secret settings document
// (config.Config) as a single JSON blob. Secrets (session token, password,
// pinned cert) stay in the system keychain, never here.
const configKey = "config"

// PutConfig serializes cfg and stores it in the meta bucket under configKey,
// making state.db the single home for all non-secret settings.
func (s *Store) PutConfig(cfg config.Config) error {
	blob, err := cfg.Marshal()
	if err != nil {
		return fmt.Errorf("store: marshal config: %w", err)
	}
	return s.update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketMeta).Put([]byte(configKey), blob); err != nil {
			return fmt.Errorf("store: write config: %w", err)
		}
		return nil
	})
}

// GetConfigBlob returns the raw stored settings JSON blob, or ErrNotFound when
// none has been written yet. Unlike GetConfig it neither decodes nor validates
// the blob, so a caller (e.g. the `unlink` recovery path) can best-effort parse
// a config that the strict GetConfig rejects as corrupt.
func (s *Store) GetConfigBlob() ([]byte, error) {
	var blob []byte
	err := s.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMeta).Get([]byte(configKey))
		if v == nil {
			return fmt.Errorf("%w: meta/%s", ErrNotFound, configKey)
		}
		// Copy: the bbolt-owned slice is only valid for the transaction's life.
		blob = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blob, nil
}

// GetConfig reads the stored settings document, returning ErrNotFound when no
// config has been written yet (an unpaired/never-configured install) so callers
// can fall back to config.Default(). A blob that will not decode (on-disk
// corruption - bbolt checksums only its meta pages) is reported wrapped in
// ErrCorruptRecord so a caller can distinguish an unrecoverable settings blob
// from a transient backend failure (and route to the `unlink` recovery path).
func (s *Store) GetConfig() (config.Config, error) {
	blob, err := s.GetConfigBlob()
	if err != nil {
		return config.Config{}, err
	}
	cfg, err := config.Unmarshal(blob)
	if err != nil {
		return config.Config{}, fmt.Errorf("store: decode config: %w: %w", ErrCorruptRecord, err)
	}
	return cfg, nil
}
