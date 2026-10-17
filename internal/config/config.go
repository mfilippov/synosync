// Package config defines the synosync settings: the configuration schema,
// platform-aware resolution of the single state directory (XDG on Linux,
// %LocalAppData% on Windows, Application Support on macOS), defaults and validation.
//
// Settings are not stored in a standalone JSON file. They are serialized as one
// JSON blob into the bbolt state database (state.db, the meta bucket's "config"
// key); this package only owns the Config type and its byte-level
// Marshal/Unmarshal. Secrets (session token, password, pinned cert) are never
// stored here - they live in the system keychain.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

// defaultServerPort is the standard Synology Drive sproto TCP port.
const defaultServerPort = 6690

// Default resource-limit values. These mirror the historical hardcoded
// constants scattered across the transport/engine/watcher packages so that an
// unconfigured install (or an older blob with no "limits" section) behaves
// exactly as before. See the Limits type for the per-field meaning.
const (
	defaultWorkers               = 3
	defaultLocalScanInterval     = 5       // seconds
	defaultDrainBatch            = 256     // ops per drain batch
	defaultUploadResumeThreshold = 4 << 20 // 4 MiB
	defaultEmptyScanThreshold    = 3       // consecutive empty-while-synced scans
	defaultWatcherDebounce       = 500     // milliseconds
	defaultColdStartCatalogCap   = 0       // 0 = unlimited
	defaultEventPollTimeout      = 300     // seconds (DESIGN §2: long-poll timeout ≥ 300 keeps idle at one blocked poll)
	defaultPollDeadlineSlack     = 300     // seconds (a DSM answers an idle 300 s poll up to 60+ s late; store v12→v13)
	defaultFullRehashInterval    = 60      // scans between forced full re-hashes
	defaultOpTurnTimeout         = 0       // seconds; 0 = per-turn deadline disabled
)

// workerMin and workerMax bound the worker-connection count, mirroring the
// transport pool's clamp range.
const (
	workerMin = 2
	workerMax = 4
)

// Lower bounds for the two limits whose too-small values are not merely unwise but
// self-defeating: each disables the protection its knob is meant to tune.
const (
	// minEmptyScanThreshold is 2 because the debounce's wall-clock floor is measured
	// BETWEEN counted observations. At 1 there is no between, and the first empty scan of a
	// view root or of a directory that still records contents propagates its deletions.
	minEmptyScanThreshold = 2
	// minOpTurnTimeout is the shortest ENABLED per-turn deadline that can complete a turn at
	// all. Below it every attempt aborts and the op wedges, which is the failure the knob
	// exists to prevent.
	minOpTurnTimeout = 5
)

// Upper bounds for the resource limits. These are generous sanity ceilings, not
// tuning recommendations: their job is to keep a hand-edited or corrupt blob
// from selecting a value that exhausts memory or overflows the int64 duration
// arithmetic (Duration(sec)*Second, Duration(ms)*Millisecond) into a negative,
// tight-looping value.
const (
	// maxUploadBytes caps UploadResumeThreshold. The single-frame
	// upload path buffers a whole sub-threshold file in memory per worker, so an
	// unbounded threshold is a per-worker OOM. 256 MiB is far above any realistic
	// small-file cutoff while bounding the buffer.
	maxUploadBytes int64 = 256 << 20
	// maxIntervalSeconds caps the second-valued intervals/timeouts (one day). It
	// sits many orders of magnitude below the int64 overflow point of
	// Duration(sec)*time.Second (~9.2e9 s) so the derived Duration can never wrap.
	maxIntervalSeconds = 24 * 60 * 60
	// maxWatcherDebounceMillis caps WatcherDebounce (one hour), likewise well below
	// the overflow point of Duration(ms)*time.Millisecond.
	maxWatcherDebounceMillis = 60 * 60 * 1000
	// maxDrainBatch caps DrainBatch. It is the only int knob the engine multiplies
	// (largeTreeThreshold = DrainBatch*20), so an absurd hand-edited value would
	// overflow that product into a negative threshold. 65536 is far above any
	// realistic concurrency batch while keeping DrainBatch*20 nowhere near the int
	// overflow point.
	maxDrainBatch = 65536
)

// Server describes how to reach a paired Synology Drive server. Secrets are not
// stored here; only the coordinates needed to open a connection.
type Server struct {
	// Host is the server hostname or IP address.
	Host string `json:"host"`
	// Port is the sproto TCP port (default 6690).
	Port int `json:"port"`
	// User is the login (account) name used for authentication.
	User string `json:"user"`
	// DsID is the server's DiskStation serial learned from the TEST_CONN probe
	// during pairing and echoed as the encrypt_channel restore id on every
	// reconnect. It is non-secret; it is recorded here so reconnects need no
	// fresh probe. Empty until the server has been paired.
	DsID string `json:"ds_id,omitempty"`
}

// Limits holds the tunable resource knobs that were formerly hardcoded
// constants in the transport, engine and watcher packages. Every field has a
// safe default equal to its historical constant value, so an unconfigured
// install (or an older blob with no "limits" section) behaves identically; a
// zero field is back-filled in applyDefaults.
type Limits struct {
	// Workers is the requested worker-connection count for the transport pool,
	// clamped to [workerMin, workerMax] at the seam.
	Workers int `json:"workers,omitempty"`
	// LocalScanInterval is the minimum delay between local filesystem scans, in
	// seconds.
	LocalScanInterval int `json:"local_scan_interval_s,omitempty"`
	// DrainBatch bounds how many queued ops the executor schedules concurrently
	// in one drain batch.
	DrainBatch int `json:"drain_batch,omitempty"`
	// UploadResumeThreshold is the file size (bytes) at or below which a push uses
	// the plain single-shot upload instead of the token-based resume_upload path.
	// It is a free choice, not a server limit: a live DSM accepts a single-shot
	// upload of at least 8 MiB (the real Drive Client nonetheless switches at
	// exactly 512 KiB). The trade-off is local: the single-shot path buffers the
	// whole file in memory, the resume path streams it off disk.
	UploadResumeThreshold int64 `json:"upload_resume_threshold,omitempty"`
	// EmptyScanThreshold is the number of consecutive empty-while-synced scans a
	// view must show before its baselines are treated as deleted.
	EmptyScanThreshold int `json:"empty_scan_threshold,omitempty"`
	// WatcherDebounce is the window over which filesystem events are coalesced
	// into a single wake, in milliseconds.
	WatcherDebounce int `json:"watcher_debounce_ms,omitempty"`
	// ColdStartCatalogCap caps the number of catalog entries a cold-start pull
	// will accumulate before aborting gracefully; 0 means unlimited.
	//
	// It ALSO clamps the server-side subtree walk an outbound DIRECTORY delete runs
	// before it applies (engine/outbound_removedir.go) - that walk is never uncapped,
	// since it runs inline on a destructive op under a worker lease and re-runs on every
	// attempt. Exceeding the cap there is a REFUSAL with a durable trace rather than a
	// retry, so a low value bounds cold-start memory at the price of refusing deletes of
	// large directories.
	ColdStartCatalogCap int `json:"cold_start_catalog_cap,omitempty"`
	// EventPollTimeout is the server-side long-poll timeout the loop asks for, in
	// seconds.
	EventPollTimeout int `json:"event_poll_timeout_s,omitempty"`
	// PollDeadlineSlack is the extra time (seconds) added to EventPollTimeout to
	// bound the client-side read deadline.
	PollDeadlineSlack int `json:"poll_deadline_slack_s,omitempty"`
	// FullRehashInterval is the number of local scans over which every file is
	// re-hashed once in full. The cheap scan otherwise reuses a baseline's stored hash
	// whenever the file's (size, mtime) is unchanged; the periodic re-hash is the
	// safety net that catches a same-second, same-size modification the cheap path
	// would miss. It is spread, about 1/N of the files per pass, so no single pass
	// re-reads the whole tree. 0 disables it (the cheap path is then always used).
	//
	// Unlike the other limits this field is NOT omitempty: its default (60) is
	// non-zero while 0 is a meaningful "disabled" value, so an explicit 0 must be
	// serialized - otherwise Unmarshal (which seeds from Default()) would re-read
	// the dropped key as 60 and a user could never persist "disabled".
	FullRehashInterval int `json:"full_rehash_interval"`
	// OpTurnTimeout bounds each bounded request/reply turn the engine runs over a
	// leased worker connection (the catalog/mutate actions plus the size-bounded
	// upload turns), in seconds; 0 disables the deadline (the default - a silent
	// server then hangs the op's worker until shutdown). When
	// enabling it, pick a value comfortably above the worst-case single-turn
	// transfer time: the largest wrapped turn moves up to UploadResumeThreshold
	// bytes (an inline upload), so a too-small timeout on a slow link would abort
	// every attempt and wedge the op the deadline was meant to protect.
	// Long-poll turns and server-sized download streams are never bounded by it.
	OpTurnTimeout int `json:"op_turn_timeout_s,omitempty"`
}

// DefaultLimits returns the Limits populated with the built-in default values
// (the historical hardcoded constants).
func DefaultLimits() Limits {
	return Limits{
		Workers:               defaultWorkers,
		LocalScanInterval:     defaultLocalScanInterval,
		DrainBatch:            defaultDrainBatch,
		UploadResumeThreshold: defaultUploadResumeThreshold,
		EmptyScanThreshold:    defaultEmptyScanThreshold,
		WatcherDebounce:       defaultWatcherDebounce,
		ColdStartCatalogCap:   defaultColdStartCatalogCap,
		EventPollTimeout:      defaultEventPollTimeout,
		PollDeadlineSlack:     defaultPollDeadlineSlack,
		FullRehashInterval:    defaultFullRehashInterval,
		OpTurnTimeout:         defaultOpTurnTimeout,
	}
}

// applyDefaults back-fills any field still at its zero value with the built-in
// default, so that an older blob without a "limits" section (or a partial one)
// yields a usable set of limits. ColdStartCatalogCap and OpTurnTimeout are
// intentionally not back-filled - their zero values (unlimited / deadline
// disabled) are the defaults.
//
// FullRehashInterval is likewise special: its zero value means "disabled", so it
// is never unconditionally back-filled. Only a wholly-absent Limits section
// (every field zero - e.g. a Limits built without DefaultLimits, or the
// engine resolving a partial Limits a test left empty) gets the default periodic
// re-hash interval; a section that predates this knob (other fields populated,
// this one zero) keeps the periodic re-hash off. The Unmarshal path seeds from
// Default() before decoding, so an older on-disk blob without this key still
// retains the default rather than reaching zero here.
func (l *Limits) applyDefaults() {
	if *l == (Limits{}) {
		l.FullRehashInterval = defaultFullRehashInterval
	}
	if l.Workers == 0 {
		l.Workers = defaultWorkers
	}
	if l.LocalScanInterval == 0 {
		l.LocalScanInterval = defaultLocalScanInterval
	}
	if l.DrainBatch == 0 {
		l.DrainBatch = defaultDrainBatch
	}
	if l.UploadResumeThreshold == 0 {
		l.UploadResumeThreshold = defaultUploadResumeThreshold
	}
	if l.EmptyScanThreshold == 0 {
		l.EmptyScanThreshold = defaultEmptyScanThreshold
	}
	if l.WatcherDebounce == 0 {
		l.WatcherDebounce = defaultWatcherDebounce
	}
	if l.EventPollTimeout == 0 {
		l.EventPollTimeout = defaultEventPollTimeout
	}
	if l.PollDeadlineSlack == 0 {
		l.PollDeadlineSlack = defaultPollDeadlineSlack
	}
}

// WithDefaults returns a copy of l with every zero field back-filled with its
// built-in default. It lets a consumer resolve a partial Limits without going
// through Unmarshal - e.g. the engine resolving the limits a test (or an older
// blob) left zero - so an unset knob behaves exactly as its historical constant.
func (l Limits) WithDefaults() Limits {
	l.applyDefaults()
	return l
}

// Validate reports whether the limits are internally consistent: the worker
// count within [workerMin, workerMax], every interval/size strictly positive,
// and the cold-start cap
// non-negative.
func (l Limits) Validate() error {
	if l.Workers < workerMin || l.Workers > workerMax {
		return fmt.Errorf("config: limits.workers %d out of range [%d,%d]", l.Workers, workerMin, workerMax)
	}
	if l.LocalScanInterval <= 0 {
		return fmt.Errorf("config: limits.local_scan_interval_s %d must be positive", l.LocalScanInterval)
	}
	if l.LocalScanInterval > maxIntervalSeconds {
		return fmt.Errorf("config: limits.local_scan_interval_s %d exceeds maximum %d", l.LocalScanInterval, maxIntervalSeconds)
	}
	if l.DrainBatch <= 0 {
		return fmt.Errorf("config: limits.drain_batch %d must be positive", l.DrainBatch)
	}
	if l.DrainBatch > maxDrainBatch {
		return fmt.Errorf("config: limits.drain_batch %d exceeds maximum %d", l.DrainBatch, maxDrainBatch)
	}
	if l.UploadResumeThreshold <= 0 {
		return fmt.Errorf("config: limits.upload_resume_threshold %d must be positive", l.UploadResumeThreshold)
	}
	if l.UploadResumeThreshold > maxUploadBytes {
		return fmt.Errorf("config: limits.upload_resume_threshold %d exceeds maximum %d", l.UploadResumeThreshold, maxUploadBytes)
	}
	if l.EmptyScanThreshold < minEmptyScanThreshold {
		// One is not a debounce. The engine's wall-clock floor is measured BETWEEN counted
		// observations, so a threshold of 1 has no "between": the first scan that finds a view
		// root - or a directory that still records contents - empty propagates the deletions
		// immediately, which is exactly the unmounted-volume shape the guard exists to survive.
		// Rejecting it here is the only place that can: the engine reads the number and cannot
		// tell a deliberate 1 from a corrupt one.
		return fmt.Errorf("config: limits.empty_scan_threshold %d must be at least %d "+
			"(a threshold of 1 accepts the first empty scan as proof and defeats the debounce)",
			l.EmptyScanThreshold, minEmptyScanThreshold)
	}
	if l.WatcherDebounce <= 0 {
		return fmt.Errorf("config: limits.watcher_debounce_ms %d must be positive", l.WatcherDebounce)
	}
	if l.WatcherDebounce > maxWatcherDebounceMillis {
		return fmt.Errorf("config: limits.watcher_debounce_ms %d exceeds maximum %d", l.WatcherDebounce, maxWatcherDebounceMillis)
	}
	if l.EventPollTimeout <= 0 {
		return fmt.Errorf("config: limits.event_poll_timeout_s %d must be positive", l.EventPollTimeout)
	}
	if l.EventPollTimeout > maxIntervalSeconds {
		return fmt.Errorf("config: limits.event_poll_timeout_s %d exceeds maximum %d", l.EventPollTimeout, maxIntervalSeconds)
	}
	if l.PollDeadlineSlack <= 0 {
		return fmt.Errorf("config: limits.poll_deadline_slack_s %d must be positive", l.PollDeadlineSlack)
	}
	if l.PollDeadlineSlack > maxIntervalSeconds {
		return fmt.Errorf("config: limits.poll_deadline_slack_s %d exceeds maximum %d", l.PollDeadlineSlack, maxIntervalSeconds)
	}
	if l.ColdStartCatalogCap < 0 {
		return fmt.Errorf("config: limits.cold_start_catalog_cap %d must be non-negative", l.ColdStartCatalogCap)
	}
	if l.FullRehashInterval < 0 {
		return fmt.Errorf("config: limits.full_rehash_interval %d must be non-negative", l.FullRehashInterval)
	}
	if l.OpTurnTimeout < 0 {
		return fmt.Errorf("config: limits.op_turn_timeout_s %d must be non-negative", l.OpTurnTimeout)
	}
	if l.OpTurnTimeout > 0 && l.OpTurnTimeout < minOpTurnTimeout {
		// An ENABLED deadline this short cannot complete even a trivial turn: the field's own
		// documentation says to pick a value comfortably above the worst-case single-turn
		// transfer, which moves up to UploadResumeThreshold bytes. Below this floor every
		// attempt aborts and the op wedges - the failure the knob was added to prevent, caused
		// by the knob. How far above the floor to go stays the operator's judgement; only the
		// values that cannot work under any setting are refused.
		return fmt.Errorf("config: limits.op_turn_timeout_s %d is too short to complete a turn; "+
			"use at least %d, or 0 to disable the deadline", l.OpTurnTimeout, minOpTurnTimeout)
	}
	if l.OpTurnTimeout > maxIntervalSeconds {
		return fmt.Errorf("config: limits.op_turn_timeout_s %d exceeds maximum %d", l.OpTurnTimeout, maxIntervalSeconds)
	}
	return nil
}

// Config is the root configuration document persisted as JSON.
type Config struct {
	// SyncRoot is the absolute path to the local directory mirrored against the
	// server.
	SyncRoot string `json:"sync_root"`
	// Server holds the paired server coordinates.
	Server Server `json:"server"`
	// LogLevel is the slog level name: "debug", "info", "warn" or "error".
	LogLevel string `json:"log_level"`
	// LogFormat is the slog output format: "text" or "json". Default "text".
	LogFormat string `json:"log_format,omitempty"`
	// DeviceUUID is the stable per-install client identifier announced to the
	// server in the client agent. It is generated on first pairing (auth package)
	// and persisted here so every later connection presents the same identity. It
	// is non-secret; session tokens and the pinned cert live in the keychain.
	// Empty until the first pairing.
	DeviceUUID string `json:"device_uuid,omitempty"`
	// Limits holds the tunable resource knobs (worker count, scan interval, drain
	// batch, upload thresholds, watcher debounce, cold-start cap, poll timeouts).
	// A zero/absent section is back-filled with the historical defaults. No
	// omitempty: encoding/json never treats a struct as empty, so the tag would be
	// a no-op - the section is always emitted.
	Limits Limits `json:"limits"`
	// Views selects which server views sync and how they land on disk. Its zero
	// value is the historical behaviour - every view the account can see, each in
	// its own subdirectory - so an install that predates the field keeps exactly
	// the layout it already has on disk. Written by `pair` and by
	// `config set views.selected`, both through the same gate.
	Views Views `json:"views"`
}

// Views records the view selection and the on-disk layout it implies.
//
// The layout decides where every synced file lives, so changing it relocates a
// tree that baselines already describe. Baselines are keyed by (view_id,
// view-relative path) and do NOT encode the view directory, so a relocation does
// not invalidate them - but the engine would look in the new place, find nothing,
// read that as a local deletion of everything, and push those deletions to the
// server. LayoutIdentity is what the writers compare to refuse that.
type Views struct {
	// Selected lists the views to sync. Empty means every view the account can
	// see: the historical behaviour, and what an install predating the field has.
	Selected []SelectedView `json:"selected,omitempty"`
	// Flat puts the single selected view's contents DIRECTLY in the sync root,
	// with no per-view subdirectory.
	//
	// It is stored, never derived from len(Selected). Deriving it made every
	// change of the selection's SIZE silently relocate the whole tree, which is a
	// data-loss path that then has to be guarded at each write site separately -
	// and one of them was missed. Stored, the layout changes only when somebody
	// changes it, and one gate covers every writer.
	//
	// Legal only with exactly one selected view: two views sharing the sync root
	// would collide path-for-path, and the executor's serialisation keys - which
	// are (view directory, path) - would stop distinguishing them.
	//
	// It also widens what the engine considers in scope. With a subdirectory the
	// sync root is only a container and a stray file beside it is invisible;
	// flattened, everything in the sync root is scanned and uploaded. Every writer
	// that turns it on must first require an empty sync root.
	Flat bool `json:"flat,omitempty"`

	// Excluded lists server paths that are not synced, each with its whole subtree
	// (`synosync exclude`). An excluded path is invisible in both directions: the server
	// walk does not enter it, its events are skipped, and the local scan neither uploads
	// nor deletes anything under it. Excluding never deletes: what was already synced stays
	// on disk, untracked. See docs/ENGINE.md, exclude.go.
	Excluded []ExcludedPath `json:"excluded,omitempty"`

	// ExcludedNames are name rules applied to every path component in every view (a matching
	// folder takes its subtree): the stock client's defaults on a new pairing, editable with
	// `synosync exclude --name`. config.SystemNames apply on top and cannot be removed.
	ExcludedNames []ExcludedName `json:"excluded_names,omitempty"`
}

// SelectedView names one selected view.
//
// The ID is the match key, and it is the server's stable identifier - the same
// one every baseline is keyed by. Matching on the NAME instead made a server-side
// rename permanently suppress the view: the engine adopts the new name into its
// store record and nothing updates the config, so the selection stopped matching
// and the daemon synced nothing, silently. A name also arrives in whatever
// Unicode normalisation the server chose, which the engine normalises at its own
// ingestion boundary and the config did not.
type SelectedView struct {
	// ID is the server's view id, and the only thing compared.
	ID uint64 `json:"id"`
	// Name is what the view was called when it was selected. Display only: it may
	// be stale after a rename, and nothing matches on it.
	Name string `json:"name,omitempty"`
}

// Default returns a Config populated with the built-in default values. The
// returned config is not valid until a SyncRoot and Server.Host/User are set.
func Default() Config {
	return Config{
		Server:    Server{Port: defaultServerPort},
		LogLevel:  "info",
		LogFormat: "text",
		Limits:    DefaultLimits(),
	}
}

// validLogLevels is the set of accepted log level names.
var validLogLevels = map[string]bool{
	"debug": true,
	"info":  true,
	"warn":  true,
	"error": true,
}

// validLogFormats is the set of accepted log output format names.
var validLogFormats = map[string]bool{
	"text": true,
	"json": true,
}

// Validate reports whether the configuration is internally consistent and
// usable. It does not touch the filesystem or network. It is the gate the
// flows that actually need a complete config run - pair/login/daemon - on top
// of the internal-consistency checks shared with ValidatePartial.
func (c Config) Validate() error {
	if c.SyncRoot == "" {
		return errors.New("config: sync_root must be set")
	}
	if c.Server.Host == "" {
		return errors.New("config: server.host must be set")
	}
	if c.Server.User == "" {
		return errors.New("config: server.user must be set")
	}
	return c.ValidatePartial()
}

// ValidatePartial reports whether the configuration is internally consistent
// while tolerating an incomplete pairing trio (sync_root, server.host and
// server.user may still be empty). It is the `config set` persistence gate:
// a fresh install legitimately builds the config one key
// at a time before the first pairing - the whole-config Validate would reject
// EVERY order of single-key sets on a fresh store, each set failing on another
// still-empty required field, an unsolvable deadlock that also blocked
// unrelated keys like log_level. Every field that IS set gets the same checks
// as Validate (absolute sync_root, keychain-key-safe host/user, port range,
// log level/format, the limits cross-invariants), so an invalid value is still
// never persisted; only the must-be-set completeness of the trio is deferred
// to the full Validate, which remains the pair/login/daemon gate - an
// incomplete blob can never reach a network turn.
func (c Config) ValidatePartial() error {
	if c.SyncRoot != "" && !filepath.IsAbs(c.SyncRoot) {
		return fmt.Errorf("config: sync_root %q must be an absolute path", c.SyncRoot)
	}
	if err := c.Views.Validate(); err != nil {
		return err
	}
	if err := checkKeyComponent("server.host", c.Server.Host); err != nil {
		return err
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("config: server.port %d is out of range", c.Server.Port)
	}
	if err := checkKeyComponent("server.user", c.Server.User); err != nil {
		return err
	}
	if !validLogLevels[c.LogLevel] {
		return fmt.Errorf("config: invalid log_level %q", c.LogLevel)
	}
	if !validLogFormats[c.LogFormat] {
		return fmt.Errorf("config: invalid log_format %q", c.LogFormat)
	}
	if err := c.Limits.Validate(); err != nil {
		return err
	}
	return nil
}

// checkKeyComponent rejects characters that would make the value unsafe as a
// keychain account-key component. The secrets layer builds its keys by joining
// the server and user with '/' ("session/<host>/<user>"), so a '/' inside either
// field would alias distinct (host, user) pairs onto one keychain entry -
// server="a/b", user="c" and server="a", user="b/c" both yield "session/a/b/c",
// mixing up the stored secrets. A NUL byte is rejected too: it is illegal in the
// store's key encoding and in keychain account strings on every backend. Validating
// at this input boundary is the cheapest place to close the aliasing.
func checkKeyComponent(field, v string) error {
	for _, r := range v {
		switch r {
		case '/':
			return fmt.Errorf("config: %s %q must not contain %q", field, v, "/")
		case 0:
			return fmt.Errorf("config: %s %q must not contain a NUL byte", field, v)
		}
	}
	return nil
}

// ValidLogLevel reports whether s is an accepted slog level name ("debug",
// "info", "warn" or "error"). It lets a caller validate just the log_level field
// without running the whole-config Validate (which would surface an unrelated
// incomplete-config error first).
func ValidLogLevel(s string) bool { return validLogLevels[s] }

// ValidLogFormat reports whether s is an accepted slog output format ("text" or
// "json"), the log_format analogue of ValidLogLevel.
func ValidLogFormat(s string) bool { return validLogFormats[s] }

// Marshal serializes the config to indented JSON.
func (c Config) Marshal() ([]byte, error) {
	return json.MarshalIndent(c, "", "  ")
}

// Unmarshal parses a config from a JSON blob, applying defaults for any field
// left at its zero value so that a partial document remains usable. It does not
// validate the result; call Validate separately. Unknown fields and trailing
// content are rejected so a malformed blob is never silently accepted.
//
// Schema-bump rule (downgrade hazard): because the decode is strict
// (DisallowUnknownFields), a blob written by a NEWER build that added a
// persisted Config field would make an OLDER build fail here with an opaque
// "unknown field" unmarshal error instead of the friendly store.ErrSchemaTooNew.
// Therefore adding (or removing/renaming) any persisted field MUST bump
// store.CurrentSchemaVersion, so an older binary rejects the database cleanly at
// Open via the version check before it ever reaches this strict decode. See
// DESIGN §5.1.
func Unmarshal(data []byte) (Config, error) {
	c := Default()
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: unmarshal: %w", err)
	}
	// Reject trailing content (garbage or a second document) so that a
	// malformed blob is not silently accepted on the strength of its prefix.
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return Config{}, errors.New("config: unmarshal: unexpected trailing content")
		}
		return Config{}, fmt.Errorf("config: unmarshal: trailing content: %w", err)
	}
	c.applyDefaults()
	return c, nil
}

// applyDefaults fills any field still at its zero value with the built-in
// default, so that a partial JSON document yields a usable config.
func (c *Config) applyDefaults() {
	if c.Server.Port == 0 {
		c.Server.Port = defaultServerPort
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogFormat == "" {
		c.LogFormat = "text"
	}
	c.Limits.applyDefaults()
}

// Validate checks the view selection's internal consistency.
func (v Views) Validate() error {
	seen := make(map[uint64]bool, len(v.Selected))
	for _, sv := range v.Selected {
		if sv.ID == 0 {
			return errors.New("config: views.selected holds a view with no id")
		}
		if seen[sv.ID] {
			return fmt.Errorf("config: views.selected lists view %d twice", sv.ID)
		}
		seen[sv.ID] = true
	}
	// Flat has no meaning for zero views and is unsafe for several: their paths
	// would collide in the sync root, and the executor's (view directory, path)
	// serialisation keys would stop telling them apart.
	if v.Flat && len(v.Selected) != 1 {
		return fmt.Errorf("config: views.flat needs exactly one selected view, got %d", len(v.Selected))
	}
	if err := validateExcluded(v.Excluded); err != nil {
		return err
	}
	return validateNames(v.ExcludedNames)
}

// LayoutIdentity describes WHAT OCCUPIES the sync root, which is the thing a
// change may not silently alter once anything is synced.
//
// It is deliberately not just the Flat flag. Swapping which single view is flat
// keeps the flag true while completely changing the sync root's occupant: the old
// view's files stay where they are, the new view finds them untracked, and
// uploads somebody's personal tree into a shared folder. Comparing the identity
// catches that with the same test that catches the flat/per-view flip.
func (v Views) LayoutIdentity() string {
	if v.Flat && len(v.Selected) == 1 {
		return fmt.Sprintf("flat:%d", v.Selected[0].ID)
	}
	return "per-view"
}

// Dir returns the sync-root-relative directory a view materialises in: its name,
// or "" when the layout is flat. The engine joins it with the sync root, and
// filepath.Join drops an empty element, so a flat layout resolves to the sync
// root itself.
func (v Views) Dir(name string) string {
	if v.Flat {
		return ""
	}
	return name
}

// Syncs reports whether the view is selected. An empty selection means every
// view, which is what an install predating the field has.
func (v Views) Syncs(id uint64) bool {
	if len(v.Selected) == 0 {
		return true
	}
	for _, sv := range v.Selected {
		if sv.ID == id {
			return true
		}
	}
	return false
}

// Names returns the selected views' display names, in selection order.
func (v Views) Names() []string {
	out := make([]string, 0, len(v.Selected))
	for _, sv := range v.Selected {
		out = append(out, sv.Name)
	}
	return out
}
