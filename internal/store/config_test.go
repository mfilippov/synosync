package store

import (
	"errors"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/config"
)

func TestGetConfigNotFoundOnFreshDB(t *testing.T) {
	s, _ := openTemp(t)
	if _, err := s.GetConfig(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfig on fresh db: got %v, want ErrNotFound", err)
	}
}

func TestPutGetConfigRoundTrip(t *testing.T) {
	s, path := openTemp(t)

	cfg := config.Default()
	cfg.SyncRoot = "/home/u/sync"
	cfg.Server.Host = "ds.example.com"
	cfg.Server.User = "alice"
	cfg.Server.DsID = "DS920+SN123"
	cfg.LogLevel = "debug"
	cfg.DeviceUUID = "uuid-abc"

	if err := s.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}

	got, err := s.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got.SyncRoot != cfg.SyncRoot || got.Server != cfg.Server ||
		got.LogLevel != cfg.LogLevel || got.DeviceUUID != cfg.DeviceUUID {
		t.Errorf("round-trip config = %+v, want %+v", got, cfg)
	}

	// Survives reopen and is overwritable.
	cfg.LogLevel = "warn"
	if err := s.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig overwrite: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got2, err := s2.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after reopen: %v", err)
	}
	if got2.LogLevel != "warn" {
		t.Errorf("log level after reopen = %q, want %q", got2.LogLevel, "warn")
	}
}

// TestGetConfigDecodeError verifies a corrupt config blob surfaces a decode
// error distinct from ErrNotFound (a downgrade, partial write, or tamper), and
// that it is tagged ErrCorruptRecord so a caller (the `unlink` recovery path) can
// tell an unrecoverable settings blob from a transient backend failure.
func TestGetConfigDecodeError(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(configKey), []byte("not json"))
	}); err != nil {
		t.Fatalf("seed corrupt blob: %v", err)
	}

	_, err := s.GetConfig()
	if err == nil {
		t.Fatal("GetConfig on corrupt blob = nil error, want decode error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfig on corrupt blob = ErrNotFound, want decode error: %v", err)
	}
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("GetConfig on corrupt blob = %v, want wrapped ErrCorruptRecord", err)
	}
}

// TestGetConfigBlobRawAndNotFound verifies GetConfigBlob returns the raw stored
// bytes without decoding - including an undecodable blob the strict GetConfig
// rejects - and reports ErrNotFound on a fresh store.
func TestGetConfigBlobRawAndNotFound(t *testing.T) {
	s, _ := openTemp(t)

	if _, err := s.GetConfigBlob(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfigBlob on fresh db: got %v, want ErrNotFound", err)
	}

	// An undecodable blob: GetConfig rejects it, GetConfigBlob returns it verbatim.
	raw := []byte(`{"server":{"host":"nas","user":"al"},"bogus":1}`)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(configKey), raw)
	}); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	if _, err := s.GetConfig(); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("GetConfig on unknown-field blob = %v, want ErrCorruptRecord", err)
	}
	blob, err := s.GetConfigBlob()
	if err != nil {
		t.Fatalf("GetConfigBlob: %v", err)
	}
	if string(blob) != string(raw) {
		t.Fatalf("GetConfigBlob = %q, want %q", blob, raw)
	}
}
