package store

import (
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// TestMigrateV8StripsUploadChunkSize pins the v8→v9 repair. The settings blob
// decodes with DisallowUnknownFields, so a document still carrying the removed
// limits.upload_chunk_size key would fail the strict decode outright - an install
// that upgraded would come up unable to read its own settings.
func TestMigrateV8StripsUploadChunkSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")

	// Build a v8 database by hand carrying the obsolete key.
	blob := []byte(`{"server":{"host":"h","port":6690,"user":"u"},"sync_root":"/tmp/x",` +
		`"log_level":"info","log_format":"text",` +
		`"limits":{"workers":3,"upload_chunk_size":1048576,"upload_resume_threshold":4194304}}`)
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		meta := tx.Bucket(bucketMeta)
		if err := meta.Put([]byte(schemaVersionKey), encodeUint64(8)); err != nil {
			return err
		}
		return meta.Put([]byte(configKey), blob)
	})
	if err != nil {
		t.Fatalf("seed v8 db: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	// Opening runs the migration; the config must then decode cleanly.
	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open (must migrate v8 -> v9): %v", err)
	}
	defer st.Close()

	cfg, err := st.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after migration: %v (the obsolete key was not stripped)", err)
	}
	if cfg.Server.Host != "h" || cfg.Limits.UploadResumeThreshold != 4194304 {
		t.Errorf("migrated config lost data: %+v", cfg)
	}
	raw, err := st.GetConfigBlob()
	if err != nil {
		t.Fatalf("GetConfigBlob: %v", err)
	}
	if strings.Contains(string(raw), "upload_chunk_size") {
		t.Errorf("stored blob still carries upload_chunk_size: %s", raw)
	}
}

// TestMigrateV8LeavesUnparsableBlob confirms the migration does not turn a
// diagnosable bad blob into a different one: config.Unmarshal owns that report.
func TestMigrateV8LeavesUnparsableBlob(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		meta := tx.Bucket(bucketMeta)
		if err := meta.Put([]byte(schemaVersionKey), encodeUint64(8)); err != nil {
			return err
		}
		return meta.Put([]byte(configKey), []byte("not json at all"))
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	_ = db.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open must not fail on a corrupt settings blob: %v", err)
	}
	defer st.Close()
	if _, err := st.GetConfig(); err == nil {
		t.Error("GetConfig on a corrupt blob must still report it")
	}
	raw, err := st.GetConfigBlob()
	if err != nil {
		t.Fatalf("GetConfigBlob: %v", err)
	}
	if string(raw) != "not json at all" {
		t.Errorf("migration rewrote a corrupt blob: %q", raw)
	}
}

// TestMigrateV12RaisesTheOldPollSlack: the stored blob carries every limit, the old default
// included, so the v12→v13 step rewrites exactly the old default 15 and keeps any other value.
func TestMigrateV12RaisesTheOldPollSlack(t *testing.T) {
	for _, tc := range []struct {
		stored string
		want   int
	}{{"15", 300}, {"40", 40}} {
		path := filepath.Join(t.TempDir(), "state.db")
		blob := []byte(`{"server":{"host":"h","port":6690,"user":"u"},"sync_root":"/tmp/x",` +
			`"log_level":"info","log_format":"text",` +
			`"limits":{"workers":3,"poll_deadline_slack_s":` + tc.stored + `}}`)
		db, err := bolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatalf("open raw db: %v", err)
		}
		err = db.Update(func(tx *bolt.Tx) error {
			for _, b := range allBuckets {
				if _, err := tx.CreateBucketIfNotExists(b); err != nil {
					return err
				}
			}
			meta := tx.Bucket(bucketMeta)
			if err := meta.Put([]byte(schemaVersionKey), encodeUint64(12)); err != nil {
				return err
			}
			return meta.Put([]byte(configKey), blob)
		})
		if err != nil {
			t.Fatalf("seed v12 db: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close raw db: %v", err)
		}
		st, err := Open(path)
		if err != nil {
			t.Fatalf("Open (must migrate v12 -> v13): %v", err)
		}
		cfg, err := st.GetConfig()
		_ = st.Close()
		if err != nil {
			t.Fatalf("GetConfig after migration: %v", err)
		}
		if cfg.Limits.PollDeadlineSlack != tc.want || cfg.Limits.Workers != 3 {
			t.Errorf("stored slack %s: migrated limits %+v, want slack %d and workers kept", tc.stored, cfg.Limits, tc.want)
		}
	}
}
