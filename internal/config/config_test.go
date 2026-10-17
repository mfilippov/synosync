package config

import (
	"strconv"
	"strings"
	"testing"
)

func validConfig() Config {
	c := Default()
	c.SyncRoot = hostAbs("home", "user", "Drive")
	c.Server.Host = "nas.example.com"
	c.Server.User = "alice"
	return c
}

func TestDefault(t *testing.T) {
	c := Default()
	if c.Server.Port != defaultServerPort {
		t.Errorf("default port = %d, want %d", c.Server.Port, defaultServerPort)
	}
	if c.LogLevel != "info" {
		t.Errorf("default log level = %q, want %q", c.LogLevel, "info")
	}
	if c.LogFormat != "text" {
		t.Errorf("default log format = %q, want %q", c.LogFormat, "text")
	}
	// A bare default must not validate (no sync root / server).
	if err := c.Validate(); err == nil {
		t.Error("Default().Validate() = nil, want error")
	}
}

func TestUnmarshalPreservesExplicitValues(t *testing.T) {
	// A document that sets port and log_level to explicit non-default values
	// must keep them; applyDefaults only fills zero-valued fields.
	const doc = `{"server":{"port":1234},"log_level":"debug"}`
	got, err := Unmarshal([]byte(doc))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Server.Port != 1234 {
		t.Errorf("port = %d, want 1234 (explicit value clobbered by default)", got.Server.Port)
	}
	if got.LogLevel != "debug" {
		t.Errorf("log level = %q, want %q (explicit value clobbered by default)", got.LogLevel, "debug")
	}
}

func TestUnmarshalFillsDefaultsForOmittedFields(t *testing.T) {
	// Omitted port/log_level fall back to the built-in defaults.
	got, err := Unmarshal([]byte(`{}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Server.Port != defaultServerPort {
		t.Errorf("port = %d, want default %d", got.Server.Port, defaultServerPort)
	}
	if got.LogLevel != "info" {
		t.Errorf("log level = %q, want default %q", got.LogLevel, "info")
	}
}

func TestMarshalUnmarshalRoundTrip(t *testing.T) {
	want := validConfig()
	want.LogLevel = "debug"

	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.SyncRoot != want.SyncRoot ||
		got.Server != want.Server ||
		got.LogLevel != want.LogLevel {
		t.Errorf("round-trip scalar mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestDeviceUUIDAndDsIDRoundTrip(t *testing.T) {
	want := validConfig()
	want.DeviceUUID = "0123456789abcdef0123456789abcdef"
	want.Server.DsID = "fedcba9876543210fedcba9876543210"

	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.DeviceUUID != want.DeviceUUID {
		t.Errorf("DeviceUUID = %q, want %q", got.DeviceUUID, want.DeviceUUID)
	}
	if got.Server.DsID != want.Server.DsID {
		t.Errorf("Server.DsID = %q, want %q", got.Server.DsID, want.Server.DsID)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestUnmarshalAppliesDefaults(t *testing.T) {
	// A partial document omitting port and log level must inherit defaults. The
	// sync root is host-absolute (see hostAbs) so the trailing Validate exercises
	// the defaults rather than tripping the absoluteness check on Windows;
	// strconv.Quote escapes the separators of a Windows path for the JSON literal.
	partial := `{"sync_root":` + strconv.Quote(hostAbs("srv", "drive")) + `,"server":{"host":"nas","user":"bob"}}`
	got, err := Unmarshal([]byte(partial))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Server.Port != defaultServerPort {
		t.Errorf("port = %d, want default %d", got.Server.Port, defaultServerPort)
	}
	if got.LogLevel != "info" {
		t.Errorf("log level = %q, want default info", got.LogLevel)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestLogFormatRoundTripAndDefault(t *testing.T) {
	// An omitted log_format inherits the "text" default.
	got, err := Unmarshal([]byte(`{}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.LogFormat != "text" {
		t.Errorf("log format = %q, want default %q", got.LogFormat, "text")
	}

	// An explicit json value round-trips through Marshal/Unmarshal.
	want := validConfig()
	want.LogFormat = "json"
	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err = Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.LogFormat != "json" {
		t.Errorf("log format = %q, want %q", got.LogFormat, "json")
	}

	// An explicit empty value is re-filled by applyDefaults.
	got, err = Unmarshal([]byte(`{"log_format":""}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.LogFormat != "text" {
		t.Errorf("log format = %q, want re-defaulted %q", got.LogFormat, "text")
	}
}

func TestUnmarshalReDefaultsExplicitZeros(t *testing.T) {
	// A document that explicitly sets port and log_level to their zero values
	// must still be re-filled by applyDefaults, not left at 0/"".
	const doc = `{"server":{"port":0},"log_level":""}`
	got, err := Unmarshal([]byte(doc))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Server.Port != defaultServerPort {
		t.Errorf("port = %d, want default %d (explicit zero not re-defaulted)", got.Server.Port, defaultServerPort)
	}
	if got.LogLevel != "info" {
		t.Errorf("log level = %q, want default info (explicit empty not re-defaulted)", got.LogLevel)
	}
}

func TestUnmarshalRejectsUnknownFields(t *testing.T) {
	const bad = `{"sync_root":"/x","nope":true}`
	if _, err := Unmarshal([]byte(bad)); err == nil {
		t.Error("Unmarshal with unknown field = nil error, want error")
	}
}

func TestUnmarshalRejectsTrailingContent(t *testing.T) {
	cases := map[string]string{
		"garbage after document": `{"sync_root":"/x"} garbage`,
		"two documents":          `{"sync_root":"/x"}{"sync_root":"/y"}`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Unmarshal([]byte(doc)); err == nil {
				t.Error("Unmarshal with trailing content = nil error, want error")
			}
		})
	}
}

func TestUnmarshalRejectsInvalidJSON(t *testing.T) {
	if _, err := Unmarshal([]byte(`not json`)); err == nil {
		t.Error("Unmarshal of invalid JSON = nil error, want error")
	}
}

func TestDefaultLimits(t *testing.T) {
	c := Default()
	want := DefaultLimits()
	if c.Limits != want {
		t.Errorf("Default().Limits = %+v, want %+v", c.Limits, want)
	}
	// The defaults must mirror the historical hardcoded constants exactly.
	if want.Workers != 3 || want.LocalScanInterval != 5 || want.DrainBatch != 256 ||
		want.UploadResumeThreshold != 4<<20 ||
		want.EmptyScanThreshold != 3 || want.WatcherDebounce != 500 ||
		want.ColdStartCatalogCap != 0 || want.EventPollTimeout != 300 ||
		want.PollDeadlineSlack != 300 || want.FullRehashInterval != 60 {
		t.Errorf("DefaultLimits drifted from historical constants: %+v", want)
	}
	// The defaults must themselves validate.
	if err := want.Validate(); err != nil {
		t.Errorf("DefaultLimits().Validate() = %v, want nil", err)
	}
}

func TestDefaultEventPollTimeoutMeetsDesignFloor(t *testing.T) {
	// DESIGN §2 mandates a long-poll timeout ≥ 300s so that idle costs one
	// blocked poll rather than 10× the reconnections. The default must meet that
	// floor and itself validate.
	l := DefaultLimits()
	if l.EventPollTimeout != 300 {
		t.Errorf("default EventPollTimeout = %d, want 300 (DESIGN §2 floor)", l.EventPollTimeout)
	}
	if err := l.Validate(); err != nil {
		t.Errorf("DefaultLimits().Validate() = %v, want nil", err)
	}
	// An older blob with no "limits" section migrates to the new 300 default, not
	// a stale 30 - applyDefaults back-fills the zero field with the current
	// default constant.
	got, err := Unmarshal([]byte(`{}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.EventPollTimeout != 300 {
		t.Errorf("zero-blob EventPollTimeout = %d, want 300", got.Limits.EventPollTimeout)
	}
}

func TestLimitsApplyDefaultsBackfillsZeroBlob(t *testing.T) {
	// An older blob with no "limits" section unmarshals to a zero Limits and
	// must be back-filled to the defaults (except ColdStartCatalogCap, whose
	// zero value is the default).
	got, err := Unmarshal([]byte(`{}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits != DefaultLimits() {
		t.Errorf("zero-blob Limits = %+v, want %+v", got.Limits, DefaultLimits())
	}
}

func TestLimitsApplyDefaultsPreservesExplicit(t *testing.T) {
	// A partial limits section keeps explicit values and fills the rest.
	const doc = `{"limits":{"workers":4,"cold_start_catalog_cap":1000}}`
	got, err := Unmarshal([]byte(doc))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.Workers != 4 {
		t.Errorf("workers = %d, want 4 (explicit clobbered)", got.Limits.Workers)
	}
	if got.Limits.ColdStartCatalogCap != 1000 {
		t.Errorf("cold start cap = %d, want 1000 (explicit clobbered)", got.Limits.ColdStartCatalogCap)
	}
	if got.Limits.LocalScanInterval != defaultLocalScanInterval {
		t.Errorf("scan interval = %d, want default %d", got.Limits.LocalScanInterval, defaultLocalScanInterval)
	}
}

func TestLimitsRoundTrip(t *testing.T) {
	want := validConfig()
	want.Limits.Workers = 2
	want.Limits.UploadResumeThreshold = 8 << 20
	want.Limits.ColdStartCatalogCap = 50000
	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits != want.Limits {
		t.Errorf("Limits round-trip = %+v, want %+v", got.Limits, want.Limits)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Limits)
		wantErr bool
	}{
		{"valid", func(*Limits) {}, false},
		{"workers below min", func(l *Limits) { l.Workers = 1 }, true},
		{"workers above max", func(l *Limits) { l.Workers = 5 }, true},
		{"workers at min", func(l *Limits) { l.Workers = workerMin }, false},
		{"workers at max", func(l *Limits) { l.Workers = workerMax }, false},
		{"scan interval zero", func(l *Limits) { l.LocalScanInterval = 0 }, true},
		{"scan interval negative", func(l *Limits) { l.LocalScanInterval = -1 }, true},
		{"drain batch zero", func(l *Limits) { l.DrainBatch = 0 }, true},
		{"drain batch too large", func(l *Limits) { l.DrainBatch = maxDrainBatch + 1 }, true},
		{"drain batch at max", func(l *Limits) { l.DrainBatch = maxDrainBatch }, false},
		{"resume threshold zero", func(l *Limits) { l.UploadResumeThreshold = 0 }, true},
		{"empty scan threshold zero", func(l *Limits) { l.EmptyScanThreshold = 0 }, true},
		{"watcher debounce zero", func(l *Limits) { l.WatcherDebounce = 0 }, true},
		{"event poll timeout zero", func(l *Limits) { l.EventPollTimeout = 0 }, true},
		{"poll deadline slack zero", func(l *Limits) { l.PollDeadlineSlack = 0 }, true},
		{"resume threshold too large", func(l *Limits) { l.UploadResumeThreshold = maxUploadBytes + 1 }, true},
		{"resume threshold at max", func(l *Limits) {
			l.UploadResumeThreshold = maxUploadBytes
		}, false},
		{"scan interval too large", func(l *Limits) { l.LocalScanInterval = maxIntervalSeconds + 1 }, true},
		{"scan interval at max", func(l *Limits) { l.LocalScanInterval = maxIntervalSeconds }, false},
		{"event poll timeout too large", func(l *Limits) { l.EventPollTimeout = maxIntervalSeconds + 1 }, true},
		{"poll deadline slack too large", func(l *Limits) { l.PollDeadlineSlack = maxIntervalSeconds + 1 }, true},
		{"watcher debounce too large", func(l *Limits) { l.WatcherDebounce = maxWatcherDebounceMillis + 1 }, true},
		{"watcher debounce at max", func(l *Limits) { l.WatcherDebounce = maxWatcherDebounceMillis }, false},
		{"cap negative", func(l *Limits) { l.ColdStartCatalogCap = -1 }, true},
		{"cap zero unlimited", func(l *Limits) { l.ColdStartCatalogCap = 0 }, false},
		{"full rehash negative", func(l *Limits) { l.FullRehashInterval = -1 }, true},
		{"full rehash zero disabled", func(l *Limits) { l.FullRehashInterval = 0 }, false},
		{"full rehash positive", func(l *Limits) { l.FullRehashInterval = 120 }, false},
		{"op turn timeout negative", func(l *Limits) { l.OpTurnTimeout = -1 }, true},
		{"op turn timeout zero disabled", func(l *Limits) { l.OpTurnTimeout = 0 }, false},
		{"op turn timeout positive", func(l *Limits) { l.OpTurnTimeout = 60 }, false},
		{"op turn timeout too large", func(l *Limits) { l.OpTurnTimeout = maxIntervalSeconds + 1 }, true},
		{"op turn timeout at max", func(l *Limits) { l.OpTurnTimeout = maxIntervalSeconds }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := DefaultLimits()
			tt.mutate(&l)
			err := l.Validate()
			if tt.wantErr && err == nil {
				t.Error("validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validate() = %v, want nil", err)
			}
		})
	}
}

// TestOpTurnTimeoutDefaultDisabledAndRoundTrip pins the op_turn_timeout_s knob's
// compatibility contract: the default is 0 (per-turn deadline disabled), so an
// older blob - with or without a limits section - decodes to exactly the historical
// behaviour and applyDefaults never back-fills it (mirroring ColdStartCatalogCap),
// while an explicit value survives a marshal/unmarshal round trip.
func TestOpTurnTimeoutDefaultDisabledAndRoundTrip(t *testing.T) {
	if got := DefaultLimits().OpTurnTimeout; got != 0 {
		t.Fatalf("default op_turn_timeout_s = %d, want 0 (disabled)", got)
	}
	old, err := Unmarshal([]byte(`{"limits":{"workers":3}}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if old.Limits.OpTurnTimeout != 0 {
		t.Errorf("older blob op_turn_timeout_s = %d, want 0 (disabled)", old.Limits.OpTurnTimeout)
	}

	want := validConfig()
	want.Limits.OpTurnTimeout = 45
	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.OpTurnTimeout != 45 {
		t.Errorf("round-trip op_turn_timeout_s = %d, want 45", got.Limits.OpTurnTimeout)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestFullRehashIntervalDefaultAndExplicit(t *testing.T) {
	// An older blob with a limits section but no full_rehash_interval key keeps
	// the safe default (periodic re-hash enabled), since Unmarshal seeds from
	// Default() before decoding.
	const old = `{"limits":{"workers":3,"local_scan_interval_s":5}}`
	got, err := Unmarshal([]byte(old))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.FullRehashInterval != defaultFullRehashInterval {
		t.Errorf("full rehash interval = %d, want default %d", got.Limits.FullRehashInterval, defaultFullRehashInterval)
	}

	// A wholly-absent limits section is back-filled to the default too.
	got, err = Unmarshal([]byte(`{}`))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.FullRehashInterval != defaultFullRehashInterval {
		t.Errorf("zero-blob full rehash interval = %d, want default %d", got.Limits.FullRehashInterval, defaultFullRehashInterval)
	}

	// An explicit zero (disabled) is preserved, not re-defaulted, because the
	// limits section carries other non-zero fields.
	const disabled = `{"limits":{"workers":3,"full_rehash_interval":0}}`
	got, err = Unmarshal([]byte(disabled))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.FullRehashInterval != 0 {
		t.Errorf("explicit zero full rehash interval = %d, want 0 (disabled)", got.Limits.FullRehashInterval)
	}

	// An explicit positive value round-trips.
	const custom = `{"limits":{"full_rehash_interval":120}}`
	got, err = Unmarshal([]byte(custom))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.FullRehashInterval != 120 {
		t.Errorf("explicit full rehash interval = %d, want 120", got.Limits.FullRehashInterval)
	}
}

func TestWithDefaultsBackfillsFullRehashFromZeroLimits(t *testing.T) {
	// A zero Limits (e.g. the engine resolving a partial Limits a test left
	// empty) gets the default periodic re-hash interval.
	got := (Limits{}).WithDefaults()
	if got.FullRehashInterval != defaultFullRehashInterval {
		t.Errorf("WithDefaults full rehash interval = %d, want default %d", got.FullRehashInterval, defaultFullRehashInterval)
	}
	if got != DefaultLimits() {
		t.Errorf("WithDefaults on zero Limits = %+v, want %+v", got, DefaultLimits())
	}
}

func TestWithDefaultsKeepsExplicitlyDisabledFullRehash(t *testing.T) {
	// A partial (non-zero) Limits that explicitly disables the periodic re-hash
	// (FullRehashInterval == 0) must NOT be back-filled to 60 - only a wholly-zero
	// Limits gets the default. This is the invariant behind the `*l == (Limits{})`
	// guard rather than a plain `if l.FullRehashInterval == 0`.
	got := Limits{Workers: 3}.WithDefaults()
	if got.FullRehashInterval != 0 {
		t.Errorf("WithDefaults full rehash interval = %d, want 0 (disabled preserved)", got.FullRehashInterval)
	}
}

func TestFullRehashIntervalDisabledRoundTrips(t *testing.T) {
	// The whole reason FullRehashInterval is not omitempty: an explicit 0
	// (disabled) set on an otherwise-default config must survive a Marshal →
	// Unmarshal round-trip rather than re-defaulting to 60. This mirrors the
	// `config set limits.full_rehash_interval 0` CLI path.
	want := Default()
	want.Limits.FullRehashInterval = 0
	data, err := want.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Limits.FullRehashInterval != 0 {
		t.Errorf("round-trip FullRehashInterval = %d, want 0 (disabled)", got.Limits.FullRehashInterval)
	}
}

func TestValidateRejectsBadLimits(t *testing.T) {
	// A whole-Config Validate must surface a bad limits section.
	c := validConfig()
	c.Limits.Workers = 99
	if err := c.Validate(); err == nil {
		t.Error("Validate() with bad limits = nil, want error")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"empty sync root", func(c *Config) { c.SyncRoot = "" }, true},
		{"relative sync root", func(c *Config) { c.SyncRoot = "relative/path" }, true},
		{"empty host", func(c *Config) { c.Server.Host = "" }, true},
		{"empty user", func(c *Config) { c.Server.User = "" }, true},
		{"slash in host", func(c *Config) { c.Server.Host = "a/b" }, true},
		{"slash in user", func(c *Config) { c.Server.User = "b/c" }, true},
		{"nul in host", func(c *Config) { c.Server.Host = "a\x00b" }, true},
		{"nul in user", func(c *Config) { c.Server.User = "a\x00b" }, true},
		{"port zero", func(c *Config) { c.Server.Port = 0 }, true},
		{"port too large", func(c *Config) { c.Server.Port = 70000 }, true},
		{"bad log level", func(c *Config) { c.LogLevel = "verbose" }, true},
		{"json log format", func(c *Config) { c.LogFormat = "json" }, false},
		{"bad log format", func(c *Config) { c.LogFormat = "yaml" }, true},
		{"empty log format", func(c *Config) { c.LogFormat = "" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			if tt.wantErr && err == nil {
				t.Error("Validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

// TestValidatePartial covers the `config set` persistence gate: an incomplete
// pairing trio passes - the legitimate pre-pairing
// state, which the whole-config Validate rejects - while every set field still
// gets the same shape/consistency checks as Validate.
func TestValidatePartial(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"fresh default (empty trio)", func(c *Config) { *c = Default() }, false},
		{"only host set", func(c *Config) { *c = Default(); c.Server.Host = "nas.example.com" }, false},
		{"only sync root set", func(c *Config) { *c = Default(); c.SyncRoot = validConfig().SyncRoot }, false},
		{"complete valid", func(*Config) {}, false},
		{"relative sync root", func(c *Config) { c.SyncRoot = "relative/path" }, true},
		{"slash in host", func(c *Config) { c.Server.Host = "a/b" }, true},
		{"slash in user", func(c *Config) { c.Server.User = "b/c" }, true},
		{"nul in host", func(c *Config) { c.Server.Host = "a\x00b" }, true},
		{"port zero", func(c *Config) { c.Server.Port = 0 }, true},
		{"port too large", func(c *Config) { c.Server.Port = 70000 }, true},
		{"bad log level", func(c *Config) { c.LogLevel = "verbose" }, true},
		{"bad log format", func(c *Config) { c.LogFormat = "yaml" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.ValidatePartial()
			if tt.wantErr && err == nil {
				t.Error("ValidatePartial() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("ValidatePartial() = %v, want nil", err)
			}
		})
	}
}

// TestLowerResumeThresholdAccepted pins what removing limits.upload_chunk_size
// unblocked: an operator matching the real Drive Client's 512 KiB switch point was
// refused, because the dead knob's default (1 MiB) was cross-checked against the
// live one. Nothing reads a chunk size any more - resume_upload carries the whole
// file in one content frame - so the constraint was pure obstruction.
func TestLowerResumeThresholdAccepted(t *testing.T) {
	c := Default()
	c.Server.Host, c.Server.User, c.SyncRoot = "h", "u", hostAbs("x")
	c.Limits.UploadResumeThreshold = 512 << 10
	if err := c.Validate(); err != nil {
		t.Fatalf("a 512 KiB resume threshold must be accepted: %v", err)
	}
}

// sel is a selection entry, for brevity in the tables below.
func sel(id uint64, name string) SelectedView { return SelectedView{ID: id, Name: name} }

// TestViewsValidate pins the one rule that keeps a flat layout safe: two views
// sharing the sync root would collide path-for-path, and the executor's (view
// directory, path) serialisation keys would stop telling them apart.
func TestViewsValidate(t *testing.T) {
	ok := []Views{
		{}, // every view, a subdirectory each
		{Selected: []SelectedView{sel(7, "home")}, Flat: true},
		{Selected: []SelectedView{sel(7, "home")}}, // one view, still in its subdirectory
		{Selected: []SelectedView{sel(7, "home"), sel(9, "Team")}},
	}
	for _, v := range ok {
		if err := v.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v, want nil", v, err)
		}
	}

	bad := map[string]Views{
		"flat with none":    {Flat: true},
		"flat with several": {Selected: []SelectedView{sel(7, "home"), sel(9, "Team")}, Flat: true},
		"duplicate id":      {Selected: []SelectedView{sel(7, "home"), sel(7, "other")}},
		"no id":             {Selected: []SelectedView{sel(0, "home")}},
	}
	for name, v := range bad {
		t.Run(name, func(t *testing.T) {
			if err := v.Validate(); err == nil {
				t.Errorf("Validate(%+v) = nil, want a refusal", v)
			}
		})
	}
}

// TestViewsLayoutIdentity is what every writer compares. It is deliberately not
// just the Flat flag: swapping WHICH single view is flat keeps the flag true
// while completely changing the sync root's occupant - the old view's files stay
// put, the new view finds them untracked, and uploads somebody's personal tree
// into a shared folder.
func TestViewsLayoutIdentity(t *testing.T) {
	perView := []Views{
		{},
		{Selected: []SelectedView{sel(7, "home"), sel(9, "Team")}},
		{Selected: []SelectedView{sel(7, "home")}}, // one view, but not flat
	}
	for _, v := range perView {
		if got := v.LayoutIdentity(); got != "per-view" {
			t.Errorf("LayoutIdentity(%+v) = %q, want per-view", v, got)
		}
	}

	home := Views{Selected: []SelectedView{sel(7, "home")}, Flat: true}
	team := Views{Selected: []SelectedView{sel(9, "Team")}, Flat: true}
	if home.LayoutIdentity() == team.LayoutIdentity() {
		t.Error("two different flat occupants share a layout identity; the swap would not be refused")
	}
	if home.LayoutIdentity() == (Views{}).LayoutIdentity() {
		t.Error("flat and per-view share a layout identity")
	}
	// The NAME is display only, so renaming the same view is not a layout change.
	renamed := Views{Selected: []SelectedView{sel(7, "Projects")}, Flat: true}
	if home.LayoutIdentity() != renamed.LayoutIdentity() {
		t.Error("a renamed view reads as a different layout; a rename would be refused as a relocation")
	}
}

// TestViewsDir covers where a view materialises. filepath.Join drops an empty
// element, so a flat layout resolves to the sync root itself.
func TestViewsDir(t *testing.T) {
	if got := (Views{}).Dir("home"); got != "home" {
		t.Errorf("Dir = %q, want home", got)
	}
	if got := (Views{Selected: []SelectedView{sel(7, "home")}, Flat: true}).Dir("home"); got != "" {
		t.Errorf("Dir = %q, want empty under a flat layout", got)
	}
}

// TestViewsSyncs pins that the match is by ID. A server-side rename must not
// change the answer - matching by name made it suppress the view for good.
func TestViewsSyncs(t *testing.T) {
	if !(Views{}).Syncs(1234) {
		t.Error("an empty selection should sync every view")
	}
	v := Views{Selected: []SelectedView{sel(7, "home"), sel(9, "Team")}}
	if !v.Syncs(7) || !v.Syncs(9) {
		t.Error("a selected view does not sync")
	}
	if v.Syncs(11) {
		t.Error("an unselected view syncs")
	}
	// The stored name is stale after a rename; the id still matches.
	stale := Views{Selected: []SelectedView{sel(7, "the old name")}}
	if !stale.Syncs(7) {
		t.Error("a renamed view stopped matching; the selection is name-keyed")
	}
}

func TestViewsNames(t *testing.T) {
	v := Views{Selected: []SelectedView{sel(7, "home"), sel(9, "Team")}}
	got := v.Names()
	if len(got) != 2 || got[0] != "home" || got[1] != "Team" {
		t.Errorf("Names = %v, want [home Team]", got)
	}
}

// TestEmptyScanThresholdOfOneIsRejected pins the floor on the debounce that protects a view
// root - and, since the nested-unmount guard reuses it, every directory inside one.
//
// The engine's wall-clock floor is measured BETWEEN counted observations, so at 1 there is no
// "between": the first scan that finds a view empty propagates its deletions, which is exactly
// the unmounted-volume shape the debounce exists to survive. The engine reads the number and
// cannot tell a deliberate 1 from a corrupt one, so this is the only place that can refuse it.
func TestEmptyScanThresholdOfOneIsRejected(t *testing.T) {
	l := DefaultLimits()
	l.EmptyScanThreshold = 1
	err := l.Validate()
	if err == nil {
		t.Fatal("a threshold of 1 was accepted; it accepts the first empty scan as proof")
	}
	if !strings.Contains(err.Error(), "empty_scan_threshold") {
		t.Errorf("error = %v, want it to name the field", err)
	}

	l.EmptyScanThreshold = 2
	if err := l.Validate(); err != nil {
		t.Errorf("two observations is the minimum real debounce and was rejected: %v", err)
	}
}

// TestOpTurnTimeoutFloorAppliesOnlyWhenEnabled pins that the per-turn deadline is either off or
// long enough to complete a turn. A value below the floor aborts every attempt and wedges the
// op - the failure the knob was added to prevent, caused by the knob.
func TestOpTurnTimeoutFloorAppliesOnlyWhenEnabled(t *testing.T) {
	l := DefaultLimits()

	l.OpTurnTimeout = 0
	if err := l.Validate(); err != nil {
		t.Errorf("0 disables the deadline and must stay valid: %v", err)
	}

	l.OpTurnTimeout = 1
	if err := l.Validate(); err == nil {
		t.Error("a one-second turn deadline was accepted")
	}

	l.OpTurnTimeout = minOpTurnTimeout
	if err := l.Validate(); err != nil {
		t.Errorf("the floor itself was rejected: %v", err)
	}
}

// TestDefaultsSatisfyTheirOwnFloors is the regression that matters most for a floor: a default
// that fails validation makes every fresh install unusable.
func TestDefaultsSatisfyTheirOwnFloors(t *testing.T) {
	if err := DefaultLimits().Validate(); err != nil {
		t.Fatalf("the shipped defaults do not validate: %v", err)
	}
}
