// Package store is the durable state layer for synosync, built on bbolt
// (go.etcd.io/bbolt, pure-Go, no cgo). It owns the single state.db file whose
// path config.Paths.StateDB() resolves, and exposes a typed, transactional API
// over the DESIGN §5 buckets: the meta key→value bucket, the views catalog, the
// files baseline with its byid index, the durable keep-both conflicts list, and a
// durable outbound queue.
//
// Every mutation runs inside a bbolt Update transaction; composite operations
// (a baseline record plus its byid index entry) are committed atomically in one
// transaction so the index can never drift from the baseline. Values are encoded
// with encoding/json (stdlib-first). The store itself never touches the network;
// the sync engine (phase 6) and watcher (phase 7) are its consumers.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// openTimeout bounds how long Open waits for the bbolt file lock before giving
// up, so a second process holding state.db fails fast instead of hanging.
const openTimeout = 5 * time.Second

// bucket names. Each corresponds to a DESIGN §5 namespace.
var (
	bucketMeta      = []byte("meta")
	bucketViews     = []byte("views")
	bucketFiles     = []byte("files")
	bucketByID      = []byte("byid")
	bucketQueue     = []byte("queue")
	bucketConflicts = []byte("conflicts")
	// bucketDeadLetters retains the durable trace of dropped (dead-lettered) ops so a
	// silently-skipped op is visible in `status` rather than only a one-shot log Warn.
	bucketDeadLetters = []byte("deadletters")
)

// allBuckets is the full set created during bootstrap, in a stable order.
var allBuckets = [][]byte{
	bucketMeta, bucketViews, bucketFiles, bucketByID, bucketQueue,
	bucketConflicts, bucketDeadLetters,
}

// CurrentSchemaVersion is the schema version this build understands. A freshly
// created database is initialised to this version; an existing database at a
// newer version is rejected by Open, and one at an older version is migrated up.
const CurrentSchemaVersion = 13

// schemaVersionKey is the meta-bucket key holding the on-disk schema version as
// a big-endian uint64.
const schemaVersionKey = "schema_version"

// ErrNotFound reports that a requested key is absent. It is distinct from a
// backend (bbolt/IO) failure so callers can tell "never written" from "store
// unavailable" via errors.Is.
var ErrNotFound = errors.New("store: not found")

// ErrSchemaTooNew reports that the on-disk schema version is newer than this
// build understands; the database was written by a future version and must not
// be migrated down or interpreted.
var ErrSchemaTooNew = errors.New("store: schema version is newer than supported")

// ErrCorruptRecord reports on-disk corruption this build cannot read through, in either of two
// places.
//
//   - A stored VALUE that does not decode (bbolt checksums only its meta pages, so a latent value
//     bit-flip surfaces here). It is wrapped into the decode-failure path of the strict
//     single-record readers (GetBaseline, GetConfig) so a caller can distinguish an unrecoverable,
//     will-never-decode record from a transient backend failure and react terminally
//     (dead-letter/freeze, or route to `unlink` recovery for a corrupt config blob) rather than
//     retrying it forever. The tolerant readers (ScanBaselinesForView, DeleteBaseline,
//     putBaselineTx) skip or heal such a record instead of surfacing this.
//   - The DATABASE ITSELF, when Open cannot get far enough to read a record at all: a
//     lost/truncated/zeroed schema_version marker, or bbolt refusing the file outright with torn
//     meta pages (ErrInvalid/ErrChecksum). Both are the same verdict for a caller - the store is
//     unreadable and no in-band repair exists - and `synosync unlink` gates its destructive
//     recover-and-recreate branch on this sentinel, so an open failure that does NOT carry it
//     leaves the operator with no cure but deleting state.db by hand.
var ErrCorruptRecord = errors.New("store: record does not decode (corrupt)")

// ErrLocked reports that the state database is held open by another process
// (the running daemon, or a concurrent pair/login). bbolt takes an exclusive
// file lock; Open maps the lock-acquisition timeout to this sentinel so callers
// can print a friendly "another synosync process is using the database" hint via
// errors.Is instead of surfacing the raw bbolt timeout.
var ErrLocked = errors.New("store: database is locked by another process")

// Store is a handle to the open state database. It is safe for concurrent use:
// bbolt serializes writers and allows concurrent readers. Construct one with
// Open and release it with Close.
type Store struct {
	db *bolt.DB
	// batch guards against a Batch closure reaching back for a *Store method, which
	// deadlocks bolt's writer lock with no timeout and no diagnosis (reentrancy.go).
	batch batchOwner
	// logger is the sink the store's best-effort corruption diagnostics (the
	// ListViews/ListConflicts/ListDeadLetters corrupt-record skips and the
	// queue-quarantine Warns) route through, installed via SetLogger. Nil selects
	// slog.Default(). An atomic pointer so SetLogger during daemon assembly cannot
	// race a concurrent store read under -race.
	logger atomic.Pointer[slog.Logger]
	// quarantined counts, monotonically for the lifetime of this handle, the queue
	// records quarantineCorruptQueueRecords moved out of the queue. A quarantine drop
	// is the ONE producer of a StatePending baseline with no op to resolve it - the
	// state both reconcile passes are built never to produce ("enqueue before baseline
	// mutation") - because the dropped record's view/path are undecodable, so no
	// engine-side dead-letter cleanup can run for it. The engine reads the counter
	// (QuarantinedOps) after each drain pass and runs its repair sweep when it has
	// advanced. Atomic because a quarantine runs on whichever
	// goroutine happened to read the queue.
	quarantined atomic.Uint64
}

// QuarantinedOps returns how many corrupt queue records this handle has quarantined
// (dropped from the queue into the dead-letter trace) since it was opened. It is a
// monotonic counter, not a live queue statistic: the engine remembers the last value
// it acted on and reads any advance as "a record left the queue without its view or
// path ever being known", which is its cue to sweep for the pending baselines that
// record's op would otherwise have resolved (engine/queuerepair.go). Per-handle and
// in-memory by design - it is a repair TRIGGER, not durable state, and the sweep it
// arms runs in the same drain pass as the quarantine that armed it.
func (s *Store) QuarantinedOps() uint64 {
	return s.quarantined.Load()
}

// SetLogger installs the logger the store's best-effort corruption diagnostics
// route through, mirroring actions.NewClientWithLogger / ipc.NewServerWithLogger /
// transport.NewPoolWithLogger. The daemon installs its rotating-file logger so the
// corrupt-record skips and queue-quarantine Warns land in the log file rather than
// a slog.Default() that Windows SCM discards and launchd never rotates - for the
// view/conflict skips there is no durable counter, so without this an operator
// sees a view silently stop syncing with no trace. A nil logger restores the
// slog.Default() fallback. Safe to call concurrently with store reads; in practice
// the daemon sets it during assembly before the engine and status goroutines
// start. The migration-time Warns in Open predate any SetLogger call and always
// use slog.Default() - acceptable, since a fresh daemon has no rotating logger
// until its config (read from this very store) is loaded.
func (s *Store) SetLogger(l *slog.Logger) {
	s.logger.Store(l)
}

// log returns the logger corruption diagnostics route through: the one installed
// by SetLogger, or slog.Default() for CLI paths and tests that never set one.
func (s *Store) log() *slog.Logger {
	if l := s.logger.Load(); l != nil {
		return l
	}
	return slog.Default()
}

// syncNewDBDirEntry is the directory-entry fsync seam Open flushes state.db's
// parent directory through after bootstrap. It defaults to fsutil.SyncDir
// (a POSIX directory fsync, a FlushFileBuffers over a backup-semantics directory
// handle on Windows, degrading to non-durable with a Warn on filesystems that
// cannot flush a directory at all - see fsutil) and is
// a package var so tests can record that the flush fires (and
// that a first-Open failure is retried by the next Open rather than
// permanently dropping the guarantee). Open calls it on every Open, not only a
// fresh one - see Open's doc comment for why the durability must not hinge on a
// one-shot fresh window.
var syncNewDBDirEntry = fsutil.SyncDir

// Open opens (creating if absent) the state database at path. It creates the
// parent directory, opens bbolt with a lock timeout, ensures every bucket from
// DESIGN §5 exists, and runs the migration runner to bring the schema up to
// CurrentSchemaVersion. A database written by a newer build fails with
// ErrSchemaTooNew; a database that holds real data but has lost its
// schema_version marker fails with ErrCorruptRecord (the caller prints the
// `synosync unlink` recovery hint) rather than being mistaken for a fresh one.
//
// Creation is durable: bbolt fsyncs the database file's
// contents, but neither the new file's entry in its parent directory nor a
// freshly created state-dir chain is flushed by anyone else - a power loss in
// that window right after the first `pair` would lose the whole database and
// re-open the orphaned-device-registration window the early device_uuid persist closed
// (the next `pair` generates a fresh device_uuid and registers a second
// device). So the state-dir chain is created via fsutil.MkdirAllDurable (the
// parent of every directory it creates is fsynced) and the database file's
// parent directory entry is fsynced after bootstrap.
//
// That entry fsync runs on EVERY Open, not only a fresh one.
// The durability must not hinge on the one-shot fresh detection: a
// transient I/O failure of the fsync on the very first (fresh) Open leaves the
// file created but its directory entry unconfirmed, and a fresh-only gate would
// then see the repeat Open as non-fresh and never retry the fsync - permanently
// dropping the guarantee. Re-fsyncing an already-durable entry on a later reopen
// is cheap (one SyncDir per Open process-lifetime) and
// idempotent, so the guarantee is retried until it holds instead of lost on the
// first miss. A genuine I/O failure (EIO/ENOSPC) fails Open (matching the fsutil
// doctrine); the capability-gap cases degrade to non-durable success inside
// SyncDir.
func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	// 0700: the state dir holds per-user data only (state.db, the daemon lock, and
	// the 0600 IPC socket, whose access-control model relies on the containing dir
	// being reachable only by the owning user).
	if err := fsutil.MkdirAllDurable(dir, 0o700); err != nil {
		return nil, fmt.Errorf("store: create state dir: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: openTimeout})
	if err != nil {
		if errors.Is(err, bolterrors.ErrTimeout) {
			return nil, fmt.Errorf("%w: %q", ErrLocked, path)
		}
		// A database whose META PAGES are gone is corrupt, and it must say so with the same
		// sentinel the record-level readers use: `synosync unlink` gates its destructive recovery
		// on ErrCorruptRecord (cmd/synosync/unlink.go), and the whole point of that branch is to
		// break the circle where the cure needs an Open that cannot succeed. The marker-corruption
		// forms below (a lost, truncated or zeroed schema_version) already reach it; the bbolt
		// layer beneath them did not, so the classic torn-write shape - power loss tearing both
		// meta pages - left every subcommand, INCLUDING unlink, reporting a raw "invalid database"
		// with no in-band cure but a manual `rm state.db`.
		//
		// Only the two verdicts that mean the bytes are unreadable are mapped. ErrVersionMismatch
		// is deliberately NOT: it says the file was written by a different bbolt format, which is
		// the bbolt-level twin of a schema DOWNGRADE, and ErrSchemaTooNew is already kept away
		// from the destructive path for exactly that reason - a store another build can still read
		// must not be wiped by this one.
		if errors.Is(err, bolterrors.ErrInvalid) || errors.Is(err, bolterrors.ErrChecksum) {
			return nil, fmt.Errorf("%w: open %q: %w", ErrCorruptRecord, path, err)
		}
		return nil, fmt.Errorf("store: open %q: %w", path, err)
	}
	s := &Store{db: db}
	if err := s.bootstrap(); err != nil {
		_ = db.Close()
		return nil, err
	}
	// After bootstrap, so the entry made durable points at a fully initialised
	// database. Unconditional (not fresh-gated) so a failed first flush is
	// retried by the next Open rather than dropped forever.
	if err := syncNewDBDirEntry(dir); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: sync state dir entry: %w", err)
	}
	return s, nil
}

// Reset clears all durable state: every bucket (the settings blob, the views
// catalog, the file baselines and their byid index, the queue, the conflicts
// list, and the meta key→values) is dropped and recreated empty, and the
// schema version is re-initialised to CurrentSchemaVersion. It is the local-state
// half of the `unlink` flow - after Reset the database looks freshly bootstrapped,
// so the install is fully unpaired. The whole reset runs in one write transaction
// so a crash leaves the database either untouched or fully cleared.
func (s *Store) Reset() error {
	return s.update(func(tx *bolt.Tx) error {
		for _, name := range allBuckets {
			if tx.Bucket(name) != nil {
				if err := tx.DeleteBucket(name); err != nil {
					return fmt.Errorf("store: drop bucket %q: %w", name, err)
				}
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return fmt.Errorf("store: recreate bucket %q: %w", name, err)
			}
		}
		// A freshly cleared database matches the current schema (all buckets just
		// recreated), so record the version directly rather than stepping migrations.
		if err := tx.Bucket(bucketMeta).Put([]byte(schemaVersionKey), encodeUint64(CurrentSchemaVersion)); err != nil {
			return fmt.Errorf("store: reset schema version: %w", err)
		}
		return nil
	})
}

// Close releases the database file and its lock. It is safe to call once.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("store: close: %w", err)
	}
	return nil
}

// bootstrap creates any missing buckets and runs migrations, all within a single
// write transaction so a crash leaves the database either untouched or fully
// initialised.
func (s *Store) bootstrap() error {
	logger := s.log()
	err := s.update(func(tx *bolt.Tx) error {
		for _, name := range allBuckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("store: create bucket %q: %w", name, err)
			}
		}
		return migrate(tx, logger)
	})
	if err != nil {
		// migrate's sentinels (e.g. ErrSchemaTooNew) must reach the caller intact.
		return err
	}
	return nil
}

// migrate brings the schema up to CurrentSchemaVersion within the bootstrap
// transaction. An empty database (no schema_version key AND no record in any bucket) is
// initialised directly to CurrentSchemaVersion (every bucket was just created);
// an existing database is stepped forward through the ordered migrations; a
// version newer than CurrentSchemaVersion is rejected with ErrSchemaTooNew; and a
// non-empty database that has LOST its schema_version marker is rejected as
// corruption (see below). logger carries the corrupt-record skip Warns.
func migrate(tx *bolt.Tx, logger *slog.Logger) error {
	meta := tx.Bucket(bucketMeta)
	raw := meta.Get([]byte(schemaVersionKey))

	var version uint64
	if raw == nil {
		// A missing schema_version key normally means a freshly created database:
		// bootstrap just created every bucket empty, so it already matches the
		// current schema - record the version and we are done. But a database that
		// already holds view or file records - or the meta settings keys, the
		// paired-but-never-synced shape - with NO schema_version marker is not
		// fresh; the marker was lost (a truncated write, bit rot). Stamping
		// CurrentSchemaVersion would silently skip every migration the real (now
		// unknowable) on-disk version needed and declare stale data "up to date"
		// - a silently-wrong verdict over real data. Refuse to open it, tagged
		// ErrCorruptRecord so the CLI/daemon print the `synosync unlink` recovery
		// hint (mirroring a corrupt config blob), rather than migrating phantom
		// data. An empty database keeps the fresh path.
		// EVERY data bucket answers the question, not just views and files. The question is
		// "does this database hold anything at all", and a queued op, a byid entry, a conflict
		// record or a dead-letter trace is data whose correct schema version is now unknowable
		// exactly like a baseline's. The narrower probe would have stamped such a database
		// CurrentSchemaVersion and drained its queue under possibly-wrong semantics. Any bucket
		// that can hold a record belongs here, so the loop reads the same list bootstrap
		// creates, minus meta - which is examined separately, since it legitimately holds the
		// schema marker itself and is judged by metaHoldsSettings instead.
		for _, b := range allBuckets {
			if bytes.Equal(b, bucketMeta) {
				continue
			}
			if bucketHasKeys(tx, b) {
				return fmt.Errorf("%w: schema_version marker missing on a non-empty database (bucket %q); "+
					"run `synosync unlink` to clear local state and re-pair", ErrCorruptRecord, b)
			}
		}
		if metaHoldsSettings(tx) {
			return fmt.Errorf("%w: schema_version marker missing on a non-empty database; run `synosync unlink` to clear local state and re-pair", ErrCorruptRecord)
		}
		if err := meta.Put([]byte(schemaVersionKey), encodeUint64(CurrentSchemaVersion)); err != nil {
			return fmt.Errorf("store: init schema version: %w", err)
		}
		return nil
	}
	// A marker of the wrong width is the same corruption class as a missing one
	// (a truncated write / bit rot over the marker itself): the real on-disk
	// version is unknowable, so tag it ErrCorruptRecord with the same `unlink`
	// recovery hint rather than a plain error that dead-ends
	// every subcommand including the recovery path.
	if len(raw) != 8 {
		return fmt.Errorf("%w: corrupt schema_version: %d bytes; run `synosync unlink` to clear local state and re-pair", ErrCorruptRecord, len(raw))
	}
	version = decodeUint64(raw)

	if version > CurrentSchemaVersion {
		return fmt.Errorf("%w: on-disk v%d, supported v%d", ErrSchemaTooNew, version, CurrentSchemaVersion)
	}

	// Apply ordered migration steps for versions below the current one.
	for v := version; v < CurrentSchemaVersion; v++ {
		step, ok := migrations[v]
		if !ok {
			// No database this build (or any predecessor) wrote records a version
			// below the oldest known migration - v0 in particular is what an 8-byte
			// zeroed marker decodes to. It is the third form of marker corruption,
			// tagged ErrCorruptRecord with the `unlink` hint like the missing and
			// truncated forms.
			return fmt.Errorf("%w: no migration from schema v%d; run `synosync unlink` to clear local state and re-pair", ErrCorruptRecord, v)
		}
		if err := step(tx, logger); err != nil {
			return fmt.Errorf("store: migrate v%d→v%d: %w", v, v+1, err)
		}
		if err := meta.Put([]byte(schemaVersionKey), encodeUint64(v+1)); err != nil {
			return fmt.Errorf("store: record schema v%d: %w", v+1, err)
		}
	}
	return nil
}

// bucketHasKeys reports whether the named bucket holds at least one key. It is
// used by migrate to tell a genuinely fresh database (all buckets empty) from a
// non-empty one that has lost its schema_version marker.
func bucketHasKeys(tx *bolt.Tx, name []byte) bool {
	b := tx.Bucket(name)
	if b == nil {
		return false
	}
	k, _ := b.Cursor().First()
	return k != nil
}

// metaHoldsSettings reports whether the meta bucket holds either durable
// settings key (the config blob or the persisted pause state). migrate consults
// it alongside the views/files probes when the schema_version marker is absent:
// a paired-but-never-synced database (config blob written by `pair`, no
// view/baseline records yet) that lost its marker is NOT fresh - fresh-stamping
// it would silently skip the migrations the blob needed (e.g. the v3→v4
// selective_sync strip, without which the strict config decode later fails and
// forces a full re-pair instead of a migration heal), so it is rejected as
// corrupt like a database with view/file records. The
// probe deliberately names the two settings keys rather than "any meta key":
// in this branch schema_version itself is absent, and no other meta keys exist.
func metaHoldsSettings(tx *bolt.Tx) bool {
	meta := tx.Bucket(bucketMeta)
	if meta == nil {
		return false
	}
	return meta.Get([]byte(configKey)) != nil || meta.Get([]byte(pausedKey)) != nil
}

// migrations maps a starting schema version to the step that upgrades it to the
// next version.
//
// Corruption-tolerance audit:
// two steps decode stored data and both skip-and-Warn an undecodable record
// instead of aborting Open - the "one bit-rotted record aborts Open forever"
// poison-pill class the skip-and-heal doctrine closed
// elsewhere. v2→v3 iterates and re-decodes the views bucket per record; v3→v4
// decodes the single config blob, whose corruption IS ultimately the
// `unlink`-recovery domain - but that recovery runs only
// AFTER a successful Open, so failing the migration on it would block the very
// recovery path it defers to (the blob is left in place for the post-Open
// strict GetConfig to tag ErrCorruptRecord). The other steps carry no
// exposure: v1→v2, v5→v6 and v6→v7 rewrite no data (bucket creation is
// handled by bootstrap; the field addition only records the bump), and v4→v5
// is a pure idempotent DeleteBucket.
var migrations = map[uint64]func(*bolt.Tx, *slog.Logger) error{
	// v12→v13 raises limits.poll_deadline_slack_s from the old default 15 to 300. A live DSM
	// answers an idle long_poll(timeout=300) late, by 20 to over 60 s and sends nothing meanwhile
	// (measured 17.10.2026 on two servers), so a 315 s client deadline cut idle polls short and
	// logged a WARN for each. TCP keepalive already detects a dead peer; the deadline is a backstop. The whole
	// settings blob is stored, the old default included, so a new default alone would never
	// reach an existing install. Only the exact old default is rewritten: any other value is an
	// operator's choice and is kept.
	12: func(tx *bolt.Tx, logger *slog.Logger) error {
		return editStoredLimits(tx, logger, "v12→v13", func(limits map[string]json.RawMessage) bool {
			if string(limits["poll_deadline_slack_s"]) != "15" {
				return false
			}
			limits["poll_deadline_slack_s"] = json.RawMessage("300")
			return true
		})
	},

	// v11→v12 changes NO data. It adds Config.Views.ExcludedNames, for the reason v10→v11
	// exists: an older binary would sync - and upload - the names the user left out.
	11: func(*bolt.Tx, *slog.Logger) error { return nil },

	// v10→v11 changes NO data. It adds Config.Views.Excluded and View.Rescan, and exists
	// for the reason v9→v10 does: an older binary would not know the exclusions, so it
	// would sync - and upload into - exactly the subtrees the user excluded. The version
	// gate makes it refuse the database instead.
	10: func(*bolt.Tx, *slog.Logger) error { return nil },

	// v9→v10 changes NO data. It exists so an older binary refuses a database that
	// carries a view selection, with ErrSchemaTooNew ("must not be migrated down")
	// rather than a misleading diagnosis.
	//
	// The new Config.Views field decides two things every on-disk path depends on:
	// which views sync at all, and whether the single selected view materialises in
	// a per-view subdirectory or directly in the sync root. A build that does not
	// know the field would compute syncRoot/<view>/... for a tree laid out flat,
	// find nothing there, and conclude every file was deleted locally - pushing
	// those deletions to the server. The settings blob decodes with
	// DisallowUnknownFields, so such a build would in fact fail before that, but it
	// would fail as "corrupt config" and send the operator to `synosync unlink`,
	// which discards local state. The version gate turns that into the accurate
	// answer, so the field is NOT exempt as an additive tolerantly-decoded one.
	9: func(*bolt.Tx, *slog.Logger) error { return nil },

	// v8→v9 REMOVES the persisted Limits.upload_chunk_size knob. The protocol has no
	// sproto-level chunking for uploads (resume_upload carries the whole file in one
	// content frame), so nothing read it - and worse, its cross-check against
	// upload_resume_threshold refused a legitimate lower threshold on the strength of
	// a knob that did nothing.
	//
	// The settings blob decodes with DisallowUnknownFields, so an existing document
	// still carrying the key would fail the strict decode outright. Strip it here,
	// while the schema version is being raised, so the very first Open of an upgraded
	// database repairs the blob before anything reads it. A blob that does not decode
	// as an object is left alone: config.Unmarshal owns reporting it as corrupt, and
	// this migration must not turn a diagnosable bad blob into a different one.
	8: func(tx *bolt.Tx, logger *slog.Logger) error {
		meta := tx.Bucket(bucketMeta)
		raw := meta.Get([]byte(configKey))
		if raw == nil {
			return nil // never paired; nothing to strip
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(raw, &doc); err != nil {
			logger.Warn("store: settings blob does not decode as an object in the v8→v9 migration; leaving it untouched", "error", err)
			return nil
		}
		limitsRaw, ok := doc["limits"]
		if !ok {
			return nil
		}
		var limits map[string]json.RawMessage
		if err := json.Unmarshal(limitsRaw, &limits); err != nil {
			logger.Warn("store: settings limits do not decode as an object in the v8→v9 migration; leaving them untouched", "error", err)
			return nil
		}
		if _, present := limits["upload_chunk_size"]; !present {
			return nil
		}
		delete(limits, "upload_chunk_size")
		enc, err := json.Marshal(limits)
		if err != nil {
			return fmt.Errorf("store: re-encode limits: %w", err)
		}
		doc["limits"] = enc
		blob, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("store: re-encode settings: %w", err)
		}
		return meta.Put([]byte(configKey), blob)
	},
	// v7→v8 adds View.RootNodeID (the view root's small integer node id, distinct
	// from the file id in NodeID). Nothing is rewritten: an existing view decodes
	// it as 0 and refreshViews re-resolves it from the next query_user_info, which
	// runs on every sync cycle. The bump exists because the field is persisted, per
	// the schema rule.
	7: func(*bolt.Tx, *slog.Logger) error { return nil },
	// v1→v2 adds the conflicts bucket. Bucket creation is already covered by
	// bootstrap (it iterates allBuckets, which now includes conflicts), so the
	// step only needs to record the version bump; the runner in migrate persists
	// it. No data is rewritten.
	1: func(*bolt.Tx, *slog.Logger) error { return nil },
	// v2→v3 adds View.InitialSyncDone (the Phase-6e cold-start flag). Every view
	// already in the database was created by pre-cold-start code and is syncing
	// incrementally, so backfill InitialSyncDone=true to spare them a redundant
	// (and expensive) full-catalog pull. A freshly created database skips this
	// step (migrate initialises it directly to CurrentSchemaVersion), so new
	// views start with the zero value false and run cold-start on first sight.
	2: func(tx *bolt.Tx, logger *slog.Logger) error {
		views := tx.Bucket(bucketViews)
		// Collect keys first; mutating a bucket while iterating its cursor is
		// unsafe in bbolt.
		type kv struct {
			key []byte
			v   View
		}
		var updates []kv
		err := views.ForEach(func(k, raw []byte) error {
			var v View
			if err := json.Unmarshal(raw, &v); err != nil {
				// A single undecodable view value must not abort the whole
				// migration and, through it, Open - that would make the database
				// (a pre-Phase-6e v2 epoch one) unopenable forever with no in-band
				// cure, blocking even the `unlink`/Reset recovery path. Skip the
				// record with a Warn, mirroring ListViews and the
				// skip-and-heal doctrine: the healthy views
				// are still backfilled, and the skipped view keeps its zero-value
				// InitialSyncDone=false, so it fails toward re-running the
				// cold-start safe-merge (which never deletes) rather than a total
				// Open failure.
				logger.Warn("store: skipping corrupt view record in v2→v3 migration",
					"view", decodeUint64(k), "error", err)
				return nil
			}
			v.InitialSyncDone = true
			updates = append(updates, kv{key: append([]byte(nil), k...), v: v})
			return nil
		})
		if err != nil {
			return err
		}
		for _, u := range updates {
			enc, err := json.Marshal(u.v)
			if err != nil {
				return fmt.Errorf("encode view %d: %w", decodeUint64(u.key), err)
			}
			if err := views.Put(u.key, enc); err != nil {
				return fmt.Errorf("write view %d: %w", decodeUint64(u.key), err)
			}
		}
		return nil
	},
	// v3→v4 removes the dead "selective_sync" placeholder field from the config
	// blob. config.Unmarshal now decodes with DisallowUnknownFields, so an older
	// blob still carrying "selective_sync" would otherwise be rejected on read.
	// The step strips the key generically (not via config.Unmarshal, which would
	// itself reject it) and is idempotent: a blob without the key is left
	// untouched, and a never-configured install (no config key) is a no-op.
	3: func(tx *bolt.Tx, logger *slog.Logger) error {
		meta := tx.Bucket(bucketMeta)
		raw := meta.Get([]byte(configKey))
		if raw == nil {
			return nil
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			// An undecodable config blob must not abort the migration and,
			// through it, Open - every subcommand INCLUDING the `unlink`
			// corrupt-blob recovery path runs only after a
			// successful Open, so failing here would leave the database
			// unopenable forever with no in-band cure but deleting state.db by
			// hand. Skip with a Warn and leave the blob in place: after Open
			// the strict GetConfig tags it ErrCorruptRecord and `unlink`
			// recovers as usual (best-effort keychain coordinates via
			// GetConfigBlob, then Reset). A blob this far gone cannot carry a
			// decodable selective_sync key anyway, so there is nothing for
			// this step to strip.
			logger.Warn("store: skipping undecodable config blob in v3→v4 migration", "error", err)
			return nil
		}
		if _, ok := fields["selective_sync"]; !ok {
			return nil
		}
		delete(fields, "selective_sync")
		// Re-marshal with the same indentation config.Marshal uses, so the
		// migrated blob keeps the on-disk format the normal write path produces
		// rather than collapsing it to compact JSON. One cosmetic divergence
		// remains: marshalling through a map alphabetizes the top-level keys,
		// whereas config.Marshal emits struct-field order - harmless, and the
		// next PutConfig rewrites the blob in the canonical order.
		enc, err := json.MarshalIndent(fields, "", "  ")
		if err != nil {
			return fmt.Errorf("encode config blob: %w", err)
		}
		if err := meta.Put([]byte(configKey), enc); err != nil {
			return fmt.Errorf("write config blob: %w", err)
		}
		return nil
	},
	// v4→v5 drops the dead "profile" bucket. It once backed the profile-digest
	// of the removed selective-sync feature and has no remaining reader, so an
	// older database that still carries it is cleaned up here. The step is
	// idempotent: a database without the bucket (e.g. one created after the
	// bucket was removed from allBuckets) is a no-op. The bucket name is a local
	// literal because it no longer exists as a symbol in the code.
	4: func(tx *bolt.Tx, _ *slog.Logger) error {
		if err := tx.DeleteBucket([]byte("profile")); err != nil &&
			!errors.Is(err, bolterrors.ErrBucketNotFound) {
			return fmt.Errorf("drop profile bucket: %w", err)
		}
		return nil
	},
	// v5→v6 adds the deadletters bucket (the durable dead-letter trace). Bucket
	// creation is already covered by bootstrap (it iterates allBuckets, which now
	// includes deadletters), so the step only records the version bump; no data is
	// rewritten.
	5: func(*bolt.Tx, *slog.Logger) error { return nil },
	// v6→v7 adds the persisted config field limits.op_turn_timeout_s (the engine's
	// per-turn action deadline). Per the DESIGN §5.1 downgrade
	// rule, any persisted Config field addition bumps the
	// schema so an older binary rejects a newer blob cleanly with ErrSchemaTooNew
	// at the version check instead of failing its strict (DisallowUnknownFields)
	// config decode with an opaque unknown-field error. The field defaults to 0
	// (disabled) and is omitempty, so no data is rewritten here - the step only
	// records the bump.
	6: func(*bolt.Tx, *slog.Logger) error { return nil },
}

// encodeUint64 renders v as an 8-byte big-endian key. Big-endian keeps bbolt's
// byte-ordered keys numerically sorted, which the views and queue buckets rely
// on for ordered iteration.
func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// decodeUint64 reads an 8-byte big-endian value produced by encodeUint64. A
// malformed slice (any length other than 8) yields 0 instead of a panic: bbolt
// checksums only its meta pages, so latent file corruption can surface a key of
// any length on a routine read, and binary.BigEndian.Uint64 would panic the
// daemon out of ScanPendingFrom/PendingOpsN/ListViews.
// Callers that must tell a real value from a malformed key use decodeUint64OK.
func decodeUint64(b []byte) uint64 {
	v, _ := decodeUint64OK(b)
	return v
}

// decodeUint64OK reads an 8-byte big-endian value produced by encodeUint64,
// reporting false for a malformed (wrong-length) slice instead of panicking.
func decodeUint64OK(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// update runs fn in a write transaction. Every store write goes through it so the Batch
// reentrancy guard has one place to stand; see reentrancy.go for what it catches.
func (s *Store) update(fn func(*bolt.Tx) error) error {
	s.batch.check("a write")
	return s.db.Update(fn)
}

// view runs fn in a read transaction, guarded the same way. A nested read is the subtler half
// of the trap: it deadlocks only when the batch's commit has to remap a grown file, so it
// survives testing and shows up under load.
func (s *Store) view(fn func(*bolt.Tx) error) error {
	s.batch.check("a read")
	return s.db.View(fn)
}

// editStoredLimits lets a migration rewrite keys of the stored settings blob's "limits" object:
// edit changes the map in place and reports whether it changed anything. A blob or limits value
// that does not decode as an object is left alone with a Warn, as in v8→v9: config.Unmarshal owns
// reporting it as corrupt after Open, and a migration must not block that recovery.
func editStoredLimits(tx *bolt.Tx, logger *slog.Logger, step string, edit func(map[string]json.RawMessage) bool) error {
	meta := tx.Bucket(bucketMeta)
	raw := meta.Get([]byte(configKey))
	if raw == nil {
		return nil // never paired; nothing to edit
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		logger.Warn("store: settings blob does not decode as an object; leaving it untouched", "migration", step, "error", err)
		return nil
	}
	limitsRaw, ok := doc["limits"]
	if !ok {
		return nil
	}
	var limits map[string]json.RawMessage
	if err := json.Unmarshal(limitsRaw, &limits); err != nil {
		logger.Warn("store: settings limits do not decode as an object; leaving them untouched", "migration", step, "error", err)
		return nil
	}
	if !edit(limits) {
		return nil
	}
	enc, err := json.Marshal(limits)
	if err != nil {
		return fmt.Errorf("store: re-encode limits: %w", err)
	}
	doc["limits"] = enc
	blob, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("store: re-encode settings: %w", err)
	}
	return meta.Put([]byte(configKey), blob)
}
