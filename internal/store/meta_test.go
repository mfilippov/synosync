package store

import (
	"testing"

	bolt "go.etcd.io/bbolt"
)

// TestSchemaVersionReportsCurrent confirms SchemaVersion returns the version the
// migration runner stamps on a freshly bootstrapped database and that it survives
// a close/reopen cycle.
func TestSchemaVersionReportsCurrent(t *testing.T) {
	s, path := openTemp(t)

	v, err := s.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version = %d, want %d", v, CurrentSchemaVersion)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v2, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion after reopen: %v", err)
	}
	if v2 != CurrentSchemaVersion {
		t.Errorf("schema version after reopen = %d, want %d", v2, CurrentSchemaVersion)
	}
}

// TestPausedRoundTrip confirms PutPaused/GetPaused round-trip both states and
// that the persisted value survives a close/reopen cycle (the daemon-restart
// case the persisted pause exists for).
func TestPausedRoundTrip(t *testing.T) {
	s, path := openTemp(t)

	if err := s.PutPaused(true); err != nil {
		t.Fatalf("PutPaused(true): %v", err)
	}
	got, err := s.GetPaused()
	if err != nil {
		t.Fatalf("GetPaused: %v", err)
	}
	if !got {
		t.Error("GetPaused after PutPaused(true) = false, want true")
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err = s2.GetPaused()
	if err != nil {
		t.Fatalf("GetPaused after reopen: %v", err)
	}
	if !got {
		t.Error("GetPaused after reopen = false, want true")
	}

	if err := s2.PutPaused(false); err != nil {
		t.Fatalf("PutPaused(false): %v", err)
	}
	got, err = s2.GetPaused()
	if err != nil {
		t.Fatalf("GetPaused after resume: %v", err)
	}
	if got {
		t.Error("GetPaused after PutPaused(false) = true, want false")
	}
}

// TestPausedAbsentDefaultsFalse confirms the tolerant decode: a store from
// before the paused key existed (or a never-paused install) reads as unpaused
// with no error.
func TestPausedAbsentDefaultsFalse(t *testing.T) {
	s, _ := openTemp(t)
	got, err := s.GetPaused()
	if err != nil {
		t.Fatalf("GetPaused on fresh store: %v", err)
	}
	if got {
		t.Error("GetPaused on fresh store = true, want false")
	}
}

// TestPausedCorruptValueTolerated confirms an unrecognized on-disk value decodes
// as unpaused rather than an error, so a stray value can never wedge daemon
// startup.
func TestPausedCorruptValueTolerated(t *testing.T) {
	s, _ := openTemp(t)
	for _, corrupt := range [][]byte{{}, {2}, {1, 1}, []byte("true")} {
		err := s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketMeta).Put([]byte(pausedKey), corrupt)
		})
		if err != nil {
			t.Fatalf("seed corrupt value %v: %v", corrupt, err)
		}
		got, err := s.GetPaused()
		if err != nil {
			t.Fatalf("GetPaused with corrupt value %v: %v", corrupt, err)
		}
		if got {
			t.Errorf("GetPaused with corrupt value %v = true, want false", corrupt)
		}
	}
}
