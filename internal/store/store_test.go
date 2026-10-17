package store

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/mfilippov/synosync/internal/config"
)

// openTemp opens a store under a fresh temp directory and registers cleanup.
func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

func TestOpenCreatesParentDirAndBuckets(t *testing.T) {
	// Path under a not-yet-existing nested directory: Open must create it.
	path := filepath.Join(t.TempDir(), "nested", "dir", "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	err = s.db.View(func(tx *bolt.Tx) error {
		for _, name := range allBuckets {
			if tx.Bucket(name) == nil {
				t.Errorf("bucket %q missing after Open", name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}

	v, err := s.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version = %d, want %d", v, CurrentSchemaVersion)
	}
}

// TestOpenFsyncsNewDatabaseEntry confirms Open flushes state.db's
// parent-directory entry through the syncNewDBDirEntry seam - bbolt fsyncs the
// file's contents but not the directory entry, so a power loss right after the
// first pairing would otherwise lose the whole database and re-open the
// orphaned-device-registration window. The flush fires on a
// repeat Open too: the durability must not hinge on the
// one-shot fresh detection, so re-fsyncing an already-durable entry (cheap,
// idempotent) is the retry that a fresh-only gate would skip after a first-Open
// failure.
func TestOpenFsyncsNewDatabaseEntry(t *testing.T) {
	var synced []string
	orig := syncNewDBDirEntry
	syncNewDBDirEntry = func(dir string) error {
		synced = append(synced, dir)
		return nil
	}
	t.Cleanup(func() { syncNewDBDirEntry = orig })

	// A not-yet-existing nested parent also exercises the durable state-dir
	// chain creation (fsutil.MkdirAllDurable) on the same Open.
	dbDir := filepath.Join(t.TempDir(), "sub")
	path := filepath.Join(dbDir, "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open (fresh): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(synced) != 1 || synced[0] != dbDir {
		t.Fatalf("fresh Open: fsync calls = %v, want exactly [%q]", synced, dbDir)
	}

	synced = nil
	s, err = Open(path)
	if err != nil {
		t.Fatalf("Open (existing): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The entry fsync is unconditional (not fresh-gated), so the repeat Open
	// flushes the same directory again.
	if len(synced) != 1 || synced[0] != dbDir {
		t.Fatalf("repeat Open: fsync calls = %v, want exactly [%q]", synced, dbDir)
	}
}

// TestOpenRetriesDirEntryFsyncAfterFailure confirms a transient I/O failure of the
// directory-entry flush on the first (fresh) Open is retried by the next Open
// rather than permanently dropped. Before the fix the file
// was already created, so the follow-up Open saw fresh=false and never fsynced
// the entry - leaving the orphaned-device window open on that install
// forever. The first Open still fails loudly with the wrapped error and the
// database closed and unlocked (proven by the retry Open not hitting a lock
// timeout), matching the fsutil genuine-I/O-failure doctrine.
func TestOpenRetriesDirEntryFsyncAfterFailure(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	orig := syncNewDBDirEntry
	syncNewDBDirEntry = func(string) error {
		calls++
		if calls == 1 {
			return boom
		}
		return nil
	}
	t.Cleanup(func() { syncNewDBDirEntry = orig })

	path := filepath.Join(t.TempDir(), "state.db")
	// First Open: the fsync fails, so Open fails with the wrapped error and the
	// database is closed (and unlocked).
	if _, err := Open(path); !errors.Is(err, boom) {
		t.Fatalf("first Open error = %v, want wrapped %v", err, boom)
	}
	// Second Open (file now exists → non-fresh): the fsync must be retried and,
	// now that the transient has cleared, succeeds.
	s, err := Open(path)
	if err != nil {
		t.Fatalf("second Open (retry): %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if calls != 2 {
		t.Fatalf("syncNewDBDirEntry calls = %d, want 2 (first fails, second retries)", calls)
	}
}

// TestOpenPersistentDirEntryFsyncFailure confirms a persistently-failing
// directory-entry flush fails every Open with the database closed and unlocked
// each time - the retry does not mask a genuine I/O failure,
// and only once the fault clears does Open succeed.
func TestOpenPersistentDirEntryFsyncFailure(t *testing.T) {
	boom := errors.New("boom")
	orig := syncNewDBDirEntry
	syncNewDBDirEntry = func(string) error { return boom }
	t.Cleanup(func() { syncNewDBDirEntry = orig })

	path := filepath.Join(t.TempDir(), "state.db")
	if _, err := Open(path); !errors.Is(err, boom) {
		t.Fatalf("first Open error = %v, want wrapped %v", err, boom)
	}
	// The retry Open re-runs the still-failing seam and fails again - but not with
	// a lock timeout, proving the first Open closed and unlocked the database.
	if _, err := Open(path); !errors.Is(err, boom) {
		t.Fatalf("second Open error = %v, want wrapped %v (retry still fails)", err, boom)
	}
	// Once the transient clears, Open succeeds and the entry is made durable.
	syncNewDBDirEntry = func(string) error { return nil }
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open after seam recovers: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestOpenCloseReopenIdempotentBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := s1.PutView(3, View{Name: "vw-1"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: bootstrap must be idempotent and data must survive.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetView(3)
	if err != nil {
		t.Fatalf("GetView after reopen: %v", err)
	}
	if got.Name != "vw-1" {
		t.Errorf("view name = %q, want %q", got.Name, "vw-1")
	}
	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version after reopen = %d, want %d", v, CurrentSchemaVersion)
	}
}

func TestOpenRejectsFutureSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Forcibly stamp a version from the future, then reopen.
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(CurrentSchemaVersion+1))
	})
	if err != nil {
		t.Fatalf("stamp future version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = Open(path)
	if !errors.Is(err, ErrSchemaTooNew) {
		t.Fatalf("Open with future schema: got %v, want ErrSchemaTooNew", err)
	}
}

func TestOpenRejectsCorruptSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), []byte("xx"))
	})
	if err != nil {
		t.Fatalf("write corrupt version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := Open(path); err == nil {
		t.Fatal("Open with corrupt schema version: want error, got nil")
	}
}

// TestMigrateV2ToV3BackfillsInitialSyncDone simulates a v2 database (a view
// stored without the InitialSyncDone field, schema_version=2), reopens, and
// confirms the v2→v3 step backfills the existing view to InitialSyncDone=true
// while a view created afterwards (under v3) starts at false.
func TestMigrateV2ToV3BackfillsInitialSyncDone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed a view, then downgrade the on-disk state to a simulated v2 by
	// stamping schema_version=2. (The view was JSON-encoded with the new field
	// defaulting to false, which is exactly how a real v2 record decodes.)
	if err := s.PutView(5, View{Name: "old"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(2))
	})
	if err != nil {
		t.Fatalf("simulate v2: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must bump to v3 and backfill the existing view.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}

	got, err := s2.GetView(5)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if !got.InitialSyncDone {
		t.Errorf("backfilled view InitialSyncDone = false, want true")
	}

	// A view created after the migration (under v3) starts at false.
	if err := s2.PutView(6, View{Name: "new"}); err != nil {
		t.Fatalf("PutView new: %v", err)
	}
	gotNew, err := s2.GetView(6)
	if err != nil {
		t.Fatalf("GetView new: %v", err)
	}
	if gotNew.InitialSyncDone {
		t.Errorf("new view InitialSyncDone = true, want false")
	}
}

// TestMigrateV2ToV3SkipsCorruptViewRecord simulates a v2 database holding one
// healthy view and one bit-rotted (undecodable) view record. Reopening must not
// wedge Open forever on the corrupt record (which would block even unlink/Reset):
// the migration skip-and-Warns the corrupt record, backfills the healthy one, and
// bumps the schema to current. Before the fix the v2→v3 step returned the first
// json.Unmarshal error, so Open failed on every attempt.
func TestMigrateV2ToV3SkipsCorruptViewRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed a healthy view, inject a corrupt view value directly under a valid
	// (8-byte) key, then downgrade the on-disk schema to a simulated v2.
	if err := s.PutView(5, View{Name: "healthy"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketViews).Put(encodeUint64(9), []byte("{not valid json")); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(2))
	})
	if err != nil {
		t.Fatalf("simulate v2 with corrupt view: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must succeed despite the corrupt record.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with corrupt view record: %v (want success - migration must skip it)", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}

	// The healthy view is backfilled to InitialSyncDone=true.
	got, err := s2.GetView(5)
	if err != nil {
		t.Fatalf("GetView healthy: %v", err)
	}
	if !got.InitialSyncDone {
		t.Errorf("backfilled healthy view InitialSyncDone = false, want true")
	}

	// The corrupt record is left in place (recoverable, not deleted) and skipped
	// by ListViews, so the healthy sibling is the only view surfaced.
	views, err := s2.ListViews()
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if len(views) != 1 || views[0].ViewID != 5 {
		t.Errorf("ListViews after migrate = %+v, want just the healthy view 5", views)
	}
}

// TestMigrateV3ToV4StripsSelectiveSync simulates a v3 database whose config blob
// still carries the now-removed "selective_sync" field (schema_version=3),
// reopens, and confirms the v3→v4 step strips the key so GetConfig - which decodes
// with DisallowUnknownFields and would otherwise reject the stale key - succeeds.
func TestMigrateV3ToV4StripsSelectiveSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed a legacy config blob carrying selective_sync and downgrade to v3.
	legacy := []byte(`{"sync_root":"/home/u/sync","server":{"host":"ds.example.com",` +
		`"port":6690,"user":"alice"},"selective_sync":{"include":["docs"]},` +
		`"log_level":"info","log_format":"text"}`)
	err = s.db.Update(func(tx *bolt.Tx) error {
		m := tx.Bucket(bucketMeta)
		if err := m.Put([]byte(configKey), legacy); err != nil {
			return err
		}
		return m.Put([]byte(schemaVersionKey), encodeUint64(3))
	})
	if err != nil {
		t.Fatalf("simulate v3: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must bump to v4 and strip selective_sync.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}

	// GetConfig must succeed (it would fail on the stale key) and the raw blob
	// must no longer contain selective_sync.
	cfg, err := s2.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after migrate: %v", err)
	}
	if cfg.Server.Host != "ds.example.com" || cfg.SyncRoot != "/home/u/sync" {
		t.Errorf("migrated config = %+v, want host/sync_root preserved", cfg)
	}
	err = s2.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketMeta).Get([]byte(configKey))
		if bytes.Contains(raw, []byte("selective_sync")) {
			t.Errorf("config blob still contains selective_sync: %s", raw)
		}
		// The rewritten blob must keep the indented on-disk format the normal
		// config.Marshal write path produces (two-space indent), not collapse to
		// compact JSON.
		if !bytes.Contains(raw, []byte("\n  ")) {
			t.Errorf("migrated config blob is not indented: %s", raw)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}

// TestMigrateV3ToV4NoConfigKey confirms the v3→v4 step is a no-op when no config
// blob has been written yet (an unpaired install upgrading in place).
func TestMigrateV3ToV4NoConfigKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(3))
	})
	if err != nil {
		t.Fatalf("simulate v3: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.GetConfig(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfig on migrated unpaired db: got %v, want ErrNotFound", err)
	}
}

// TestMigrateV3ToV4SkipsCorruptConfigBlob simulates a v3 database whose config
// blob is bit-rotted (not valid JSON) and confirms the v3→v4 step skips it with
// a Warn instead of failing the migration - which would make Open fail forever,
// blocking every subcommand INCLUDING the `unlink` corrupt-blob recovery path,
// which runs only after a successful Open. The blob is left
// in place so the post-Open strict GetConfig tags it ErrCorruptRecord and the
// standard `unlink --yes` recovery applies: best-effort coordinate parse via
// GetConfigBlob, then Reset.
func TestMigrateV3ToV4SkipsCorruptConfigBlob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	corrupt := []byte(`{"sync_root":"/home/u/sync","server":{"host":"ds.exam`)
	err = s.db.Update(func(tx *bolt.Tx) error {
		m := tx.Bucket(bucketMeta)
		if err := m.Put([]byte(configKey), corrupt); err != nil {
			return err
		}
		return m.Put([]byte(schemaVersionKey), encodeUint64(3))
	})
	if err != nil {
		t.Fatalf("simulate v3 with corrupt blob: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must succeed despite the corrupt blob.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen with corrupt config blob: %v (want success - migration must skip it)", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}

	// The blob is left in place, so the strict GetConfig tags it ErrCorruptRecord
	// - the signal that routes `unlink` onto its recovery path.
	if _, err := s2.GetConfig(); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("GetConfig after migrate: got %v, want ErrCorruptRecord", err)
	}
	// The raw blob is still readable for the recovery path's best-effort
	// coordinate parse.
	blob, err := s2.GetConfigBlob()
	if err != nil {
		t.Fatalf("GetConfigBlob after migrate: %v", err)
	}
	if !bytes.Equal(blob, corrupt) {
		t.Errorf("GetConfigBlob = %q, want the corrupt blob left in place %q", blob, corrupt)
	}

	// The `unlink --yes` end-state: Reset leaves the install freshly unpaired.
	if err := s2.Reset(); err != nil {
		t.Fatalf("Reset (the unlink recovery step): %v", err)
	}
	if _, err := s2.GetConfig(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfig after Reset: got %v, want ErrNotFound", err)
	}
}

// TestMigrateV4ToV5DropsProfileBucket simulates a v4 database that still carries
// the now-dead "profile" bucket (schema_version=4), reopens, and confirms the
// v4→v5 step drops the bucket and bumps the schema to v5.
func TestMigrateV4ToV5DropsProfileBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Recreate the legacy profile bucket and downgrade to v4. Seed unrelated data
	// that must survive the migration.
	if err := s.PutView(5, View{Name: "keep"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("profile"))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("profile_digest"), []byte("sha256:dead")); err != nil {
			return err
		}
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(4))
	})
	if err != nil {
		t.Fatalf("simulate v4: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: migration must bump to v5 and remove the profile bucket.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}
	err = s2.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("profile")) != nil {
			t.Error("profile bucket still present after v4→v5 migration")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	// Unrelated data survived.
	got, err := s2.GetView(5)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if got.Name != "keep" {
		t.Errorf("view name = %q, want %q", got.Name, "keep")
	}
}

// TestFreshDatabaseHasNoProfileBucket confirms a freshly bootstrapped database is
// at the current schema and never creates the dead profile bucket.
func TestFreshDatabaseHasNoProfileBucket(t *testing.T) {
	s, _ := openTemp(t)

	v, err := s.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version = %d, want %d", v, CurrentSchemaVersion)
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("profile")) != nil {
			t.Error("fresh database has a profile bucket; want none")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}

// TestMigrateV4ToV5NoProfileBucket exercises the v4→v5 step's idempotency branch:
// a v4 database that never carried the dead "profile" bucket (e.g. one created
// after the bucket was dropped from allBuckets) must migrate cleanly, with the
// step swallowing bolt.ErrBucketNotFound rather than failing the upgrade.
func TestMigrateV4ToV5NoProfileBucket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Downgrade to v4 without ever creating a profile bucket. Bootstrap (and the
	// fresh-DB path) never makes one, so deleting it must be a tolerated no-op.
	err = s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("profile")) != nil {
			return errors.New("precondition: profile bucket unexpectedly present")
		}
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(4))
	})
	if err != nil {
		t.Fatalf("simulate v4: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen (v4→v5 with no profile bucket): %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}
}

// TestMigrateV6ToV7RecordsBump simulates a v6 database (one written before the
// limits.op_turn_timeout_s config field existed) and confirms
// the recording-only v6→v7 step bumps the version while leaving the stored config
// blob readable and byte-equivalent in meaning: the new field is omitempty with a
// 0 (disabled) default, so no data rewrite is needed.
func TestMigrateV6ToV7RecordsBump(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	cfg := config.Default()
	cfg.SyncRoot = "/home/u/sync"
	cfg.Server.Host = "ds.example.com"
	cfg.Server.User = "alice"
	if err := s.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(6))
	})
	if err != nil {
		t.Fatalf("simulate v6: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen (v6→v7): %v", err)
	}
	defer s2.Close()

	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Fatalf("schema version after migrate = %d, want %d", v, CurrentSchemaVersion)
	}
	got, err := s2.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after migrate: %v", err)
	}
	if got.SyncRoot != cfg.SyncRoot || got.Limits.OpTurnTimeout != 0 {
		t.Errorf("migrated config = %+v, want preserved blob with op_turn_timeout_s 0", got)
	}
}

// TestMigrationsMapHasSteps guards that the runner has a step for every version
// below current; a missing entry would make migrate reject a real older database.
func TestMigrationsMapHasSteps(t *testing.T) {
	for v := uint64(1); v < CurrentSchemaVersion; v++ {
		if _, ok := migrations[v]; !ok {
			t.Fatalf("missing v%d→v%d migration step", v, v+1)
		}
	}
}

// TestOpenRejectsMissingSchemaVersionOnNonEmptyDB confirms that a database holding
// real data but missing its schema_version marker (a truncated write / bit rot) is
// rejected as corruption rather than mistaken for a fresh database and stamped
// straight to CurrentSchemaVersion - which would silently skip every migration the
// real (now unknowable) on-disk version needed and declare stale data "up to date".
// Before the fix Open reopened it silently as fresh.
func TestOpenRejectsMissingSchemaVersionOnNonEmptyDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Seed real data, then drop the schema_version marker.
	if err := s.PutView(5, View{Name: "v"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Delete([]byte(schemaVersionKey))
	})
	if err != nil {
		t.Fatalf("drop schema_version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: must be rejected as corruption, not silently treated as fresh.
	_, err = Open(path)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("reopen non-empty db without schema_version: got %v, want ErrCorruptRecord", err)
	}
	if !strings.Contains(err.Error(), "unlink") {
		t.Errorf("Open error %q, want an `unlink` recovery hint", err)
	}
}

// TestOpenTagsCorruptSchemaMarkerForms confirms the two neighbouring forms of a
// damaged schema_version marker - a truncated (non-8-byte) value and an 8-byte
// zeroed marker (decoding to schema v0, below any known migration) - are tagged
// ErrCorruptRecord with the `unlink` recovery hint, like the missing-marker form.
// Before the fix both surfaced as plain errors, so unlink's
// corrupt-store recovery never triggered and the hint inside the error pointed
// back at the same failing Open - a circular dead-end.
func TestOpenTagsCorruptSchemaMarkerForms(t *testing.T) {
	cases := []struct {
		name   string
		marker []byte
	}{
		{"truncated", []byte{0x01, 0x02, 0x03}},
		{"zeroed", make([]byte, 8)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := s.PutView(5, View{Name: "v"}); err != nil {
				t.Fatalf("PutView: %v", err)
			}
			err = s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), tc.marker)
			})
			if err != nil {
				t.Fatalf("damage schema_version: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			_, err = Open(path)
			if !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("reopen with %s marker: got %v, want ErrCorruptRecord", tc.name, err)
			}
			if !strings.Contains(err.Error(), "unlink") {
				t.Errorf("Open error %q, want an `unlink` recovery hint", err)
			}
		})
	}
}

// TestOpenRejectsMissingMarkerOnMetaOnlyDB confirms the missing-marker
// non-emptiness probe also counts the meta settings keys (config blob, persisted
// pause state): a paired-but-never-synced database (config written by `pair`, no
// view/baseline records yet) whose marker was lost must be rejected as corrupt,
// not fresh-stamped - the fresh path would silently skip the migrations the blob
// needed and the strict config decode would later force a full re-pair.
// Before the fix only views/files were probed.
func TestOpenRejectsMissingMarkerOnMetaOnlyDB(t *testing.T) {
	seeds := []struct {
		name string
		seed func(*testing.T, *Store)
	}{
		{"config", func(t *testing.T, s *Store) {
			t.Helper()
			cfg := config.Default()
			cfg.SyncRoot = "/tmp/sync"
			cfg.Server.Host = "nas.example"
			cfg.Server.User = "alice"
			if err := s.PutConfig(cfg); err != nil {
				t.Fatalf("PutConfig: %v", err)
			}
		}},
		{"paused", func(t *testing.T, s *Store) {
			t.Helper()
			if err := s.PutPaused(true); err != nil {
				t.Fatalf("PutPaused: %v", err)
			}
		}},
	}
	for _, tc := range seeds {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			tc.seed(t, s)
			err = s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketMeta).Delete([]byte(schemaVersionKey))
			})
			if err != nil {
				t.Fatalf("drop schema_version: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			_, err = Open(path)
			if !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("reopen meta-only db without schema_version: got %v, want ErrCorruptRecord", err)
			}
		})
	}
}

// TestOpenTreatsMissingSchemaVersionOnEmptyDBAsFresh confirms the missing-marker
// corruption check does not misfire on a genuinely empty database: an empty store
// whose schema_version key is absent keeps the fresh path and is stamped to the
// current version.
func TestOpenTreatsMissingSchemaVersionOnEmptyDBAsFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Drop the marker on an otherwise-empty database (no views/files records).
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMeta).Delete([]byte(schemaVersionKey))
	})
	if err != nil {
		t.Fatalf("drop schema_version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen empty db without schema_version: %v (want fresh path)", err)
	}
	defer s2.Close()
	v, err := s2.SchemaVersion()
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != CurrentSchemaVersion {
		t.Errorf("schema version = %d, want %d", v, CurrentSchemaVersion)
	}
}

// TestSetLoggerRoutesCorruptViewSkipWarn confirms a corrupt-record skip Warn goes to
// the logger installed via SetLogger (the daemon's rotating file) rather than only
// slog.Default() - the view/conflict skips have no durable counter, so this Warn is
// the sole observable of a view that silently stopped syncing.
func TestSetLoggerRoutesCorruptViewSkipWarn(t *testing.T) {
	s, _ := openTemp(t)
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	// Inject a corrupt view value under a valid 8-byte key.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketViews).Put(encodeUint64(9), []byte("{not valid json"))
	})
	if err != nil {
		t.Fatalf("inject corrupt view: %v", err)
	}
	if _, err := s.ListViews(); err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if !strings.Contains(buf.String(), "skipping corrupt view record") {
		t.Errorf("corrupt-view Warn did not reach the installed logger; buf=%q", buf.String())
	}
}

// TestSetLoggerRoutesQueueQuarantineWarn confirms the queue-quarantine Warn (a corrupt
// queue record moved to the dead-letter bucket) reaches the installed logger too.
func TestSetLoggerRoutesQueueQuarantineWarn(t *testing.T) {
	s, _ := openTemp(t)
	var buf bytes.Buffer
	s.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	// Inject a corrupt queue record under a valid 8-byte seq key.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketQueue).Put(encodeUint64(1), []byte("{not valid json"))
	})
	if err != nil {
		t.Fatalf("inject corrupt queue record: %v", err)
	}
	// A bounded read skips the record and quarantines it into the dead-letter bucket.
	if _, err := s.PendingOpsN(16); err != nil {
		t.Fatalf("PendingOpsN: %v", err)
	}
	if !strings.Contains(buf.String(), "corrupt queue record dead-lettered") {
		t.Errorf("queue-quarantine Warn did not reach the installed logger; buf=%q", buf.String())
	}
}

func TestUint64KeyRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 42, 1 << 32, ^uint64(0)} {
		if got := decodeUint64(encodeUint64(v)); got != v {
			t.Errorf("round-trip %d = %d", v, got)
		}
	}
}

func TestOpenLockedFileTimesOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// A second Open on the same file must fail (lock held) rather than hang
	// forever; openTimeout bounds the wait, and the bbolt timeout maps to the
	// friendly ErrLocked sentinel.
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open on locked file: got %v, want ErrLocked", err)
	}
}

// TestOpenReportsTornMetaPagesAsCorrupt pins the recovery path for the classic torn-write shape:
// power loss leaves the two bbolt META PAGES unreadable, so bolt.Open itself fails and every
// subcommand dies before the store exists.
//
// The sentinel is what makes that recoverable. `synosync unlink` takes its destructive
// recover-and-recreate branch only for ErrCorruptRecord and otherwise reports and exits, so an
// unmapped open failure is a circle: the cure the error message recommends needs an Open that
// cannot succeed, and the only way out is deleting state.db by hand. The record-level readers and
// the schema-marker checks already reach the sentinel; this is the layer beneath them.
func TestOpenReportsTornMetaPagesAsCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Overwrite both meta pages (page 0 and page 1) with junk, leaving the rest of the file - the
	// shape a torn write leaves behind, rather than a file that was never a database.
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open the db file: %v", err)
	}
	junk := bytes.Repeat([]byte{0xA5}, 2*os.Getpagesize())
	if _, err := f.WriteAt(junk, 0); err != nil {
		t.Fatalf("scribble over the meta pages: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close the db file: %v", err)
	}

	_, err = Open(path)
	if err == nil {
		t.Fatal("Open of a database with torn meta pages: want an error, got nil")
	}
	if !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("Open error = %v, want it to carry ErrCorruptRecord: `synosync unlink` gates its "+
			"recovery on that sentinel, so an unmapped open failure leaves the store unopenable "+
			"forever with no in-band cure", err)
	}
}

// TestOpenRejectsMissingMarkerOnEveryDataBucket confirms the missing-marker
// non-emptiness probe reads EVERY data bucket, not just views/files/meta. A
// database holding only queued ops, a byid index entry, a keep-both conflict
// record or a dead-letter trace is not fresh either: its real (now unknowable)
// schema version may be older than this build's, and the fresh path would stamp
// CurrentSchemaVersion and then drain that queue and read those records under
// migrations that never ran. Before the fix each of these opened clean.
func TestOpenRejectsMissingMarkerOnEveryDataBucket(t *testing.T) {
	seeds := []struct {
		name string
		seed func(*testing.T, *Store)
	}{
		{"queue", func(t *testing.T, s *Store) {
			t.Helper()
			if _, err := s.Enqueue(Op{Type: OpUpload, Direction: DirOutbound, ViewID: 7, Path: "a.txt"}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
		}},
		{"conflicts", func(t *testing.T, s *Store) {
			t.Helper()
			if err := s.PutConflict(1, Conflict{CanonicalPath: "p", CopyFileID: "cid"}); err != nil {
				t.Fatalf("PutConflict: %v", err)
			}
		}},
		{"deadletters", func(t *testing.T, s *Store) {
			t.Helper()
			if _, err := s.RecordDeadLetter(DeadLetter{ViewID: 1, Path: "p", RecordedAt: 1}); err != nil {
				t.Fatalf("RecordDeadLetter: %v", err)
			}
		}},
		{"byid", func(t *testing.T, s *Store) {
			// Written raw: every public writer of byid also writes the files bucket,
			// which the probe already covered, so an index entry orphaned by a partial
			// older schema is the shape only a direct write reproduces.
			t.Helper()
			err := s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketByID).Put([]byte("fid"), []byte("a.txt"))
			})
			if err != nil {
				t.Fatalf("seed byid: %v", err)
			}
		}},
	}
	for _, tc := range seeds {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			s, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			tc.seed(t, s)
			err = s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketMeta).Delete([]byte(schemaVersionKey))
			})
			if err != nil {
				t.Fatalf("drop schema_version: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			_, err = Open(path)
			if !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("reopen db with only %s records and no schema_version: got %v, want ErrCorruptRecord", tc.name, err)
			}
		})
	}
}
