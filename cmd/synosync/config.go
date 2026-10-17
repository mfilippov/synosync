package main

// The `config` subcommand reads and writes the non-secret settings blob stored
// in the state database (state.db's meta/"config" key), the same home pair/login
// use. `config get` with no key prints every settable scalar field as key=value;
// `config get <key>` prints one value; `config set <key> <value>` updates one
// field, validating the field plus the config's internal consistency
// (config.ValidatePartial) before persisting so an invalid value is never saved
// - but NOT the completeness of the host/user/sync_root pairing trio, which
// legitimately stays incomplete until `pair` (the
// whole-config Validate deadlocked every fresh install - each single-key set
// failed on another still-empty required field, in every call order).
// Only a fixed whitelist of scalar keys is exposed. Side effects route
// through the shared openConfigStore injection, and the command is unit-tested
// against a temp state database. When the store is locked by a running daemon,
// `config get` falls back to the daemon's read-only `config` IPC method
// - a direct read-only bbolt open cannot work there, since
// its shared file lock conflicts with the daemon's exclusive one - while
// `config set` keeps the friendly "stop the daemon" hint: mutating settings
// under a live daemon would not take effect until restart anyway.

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// configKey describes one settable scalar field: how to read its value as a
// string, how to apply a new string value onto a *config.Config (parsing the
// scalar shape only) and how to validate just that field. The field-specific
// validate runs before the whole-config Validate so a bad value reports the
// field actually at fault (e.g. "invalid log_level") rather than the first
// unrelated incomplete-config error ("sync_root must be set").
type configKey struct {
	name     string
	get      func(config.Config) string
	set      func(*config.Config, string) error
	validate func(config.Config) error
}

// validateLimits validates only the limits sub-struct, used as the field-level
// validator for every limits.* key so a single bad knob reports a limits error
// rather than an unrelated incomplete-config one.
func validateLimits(c config.Config) error { return c.Limits.Validate() }

// configKeys is the whitelist of settable scalar fields, in a stable order.
var configKeys = []configKey{
	{
		// views.selected is the one settable key whose value decides where files
		// live rather than how the client behaves, so configSet gives it an extra
		// gate: a change that would relocate an already-synced tree is refused
		// (see refuseViewLayoutChange).
		name: "views.selected",
		get: func(c config.Config) string {
			if len(c.Views.Selected) == 0 {
				return allViews
			}
			return strings.Join(c.Views.Names(), ",")
		},
		// set is a no-op placeholder: the selection needs the STORE to resolve a
		// name to the view id it is matched by, which a configKey setter has no
		// access to. configSet routes this key to setSelectedViews before the
		// table is consulted.
		set:      func(*config.Config, string) error { return nil },
		validate: func(c config.Config) error { return c.Views.Validate() },
	},
	{
		name: "log_level",
		get:  func(c config.Config) string { return c.LogLevel },
		set:  func(c *config.Config, v string) error { c.LogLevel = v; return nil },
		validate: func(c config.Config) error {
			if !config.ValidLogLevel(c.LogLevel) {
				return fmt.Errorf("invalid log_level %q", c.LogLevel)
			}
			return nil
		},
	},
	{
		name: "log_format",
		get:  func(c config.Config) string { return c.LogFormat },
		set:  func(c *config.Config, v string) error { c.LogFormat = v; return nil },
		validate: func(c config.Config) error {
			if !config.ValidLogFormat(c.LogFormat) {
				return fmt.Errorf("invalid log_format %q", c.LogFormat)
			}
			return nil
		},
	},
	{
		name: "sync_root",
		get:  func(c config.Config) string { return c.SyncRoot },
		set:  func(c *config.Config, v string) error { c.SyncRoot = v; return nil },
		validate: func(c config.Config) error {
			if c.SyncRoot == "" {
				return errors.New("sync_root must be set")
			}
			if !filepath.IsAbs(c.SyncRoot) {
				return fmt.Errorf("sync_root %q must be an absolute path", c.SyncRoot)
			}
			// Refuse a root that would contain synosync's own state or logs. With a single
			// selected view the sync root IS the view, so the scanner would walk state.db
			// while bbolt holds it open, upload this device's pairing to the server, and
			// have every write to the log wake the watcher that reads them again.
			if paths, perr := resolveDaemonPaths(); perr == nil {
				if err := config.CheckSyncRootLocation(c.SyncRoot, paths); err != nil {
					return err
				}
			}
			return nil
		},
	},
	{
		name: "server.host",
		get:  func(c config.Config) string { return c.Server.Host },
		set:  func(c *config.Config, v string) error { c.Server.Host = v; return nil },
		validate: func(c config.Config) error {
			if c.Server.Host == "" {
				return errors.New("server.host must be set")
			}
			return nil
		},
	},
	{
		name: "server.port",
		get:  func(c config.Config) string { return strconv.Itoa(c.Server.Port) },
		set: func(c *config.Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("server.port %q is not a number", v)
			}
			c.Server.Port = n
			return nil
		},
		validate: func(c config.Config) error {
			if c.Server.Port <= 0 || c.Server.Port > 65535 {
				return fmt.Errorf("server.port %d is out of range", c.Server.Port)
			}
			return nil
		},
	},
	{
		name: "server.user",
		get:  func(c config.Config) string { return c.Server.User },
		set:  func(c *config.Config, v string) error { c.Server.User = v; return nil },
		validate: func(c config.Config) error {
			if c.Server.User == "" {
				return errors.New("server.user must be set")
			}
			return nil
		},
	},
	intKey("limits.workers",
		func(c *config.Config) *int { return &c.Limits.Workers }),
	intKey("limits.local_scan_interval_s",
		func(c *config.Config) *int { return &c.Limits.LocalScanInterval }),
	intKey("limits.drain_batch",
		func(c *config.Config) *int { return &c.Limits.DrainBatch }),
	int64Key("limits.upload_resume_threshold",
		func(c *config.Config) *int64 { return &c.Limits.UploadResumeThreshold }),
	intKey("limits.empty_scan_threshold",
		func(c *config.Config) *int { return &c.Limits.EmptyScanThreshold }),
	intKey("limits.watcher_debounce_ms",
		func(c *config.Config) *int { return &c.Limits.WatcherDebounce }),
	intKey("limits.cold_start_catalog_cap",
		func(c *config.Config) *int { return &c.Limits.ColdStartCatalogCap }),
	intKey("limits.event_poll_timeout_s",
		func(c *config.Config) *int { return &c.Limits.EventPollTimeout }),
	intKey("limits.poll_deadline_slack_s",
		func(c *config.Config) *int { return &c.Limits.PollDeadlineSlack }),
	intKey("limits.full_rehash_interval",
		func(c *config.Config) *int { return &c.Limits.FullRehashInterval }),
	intKey("limits.op_turn_timeout_s",
		func(c *config.Config) *int { return &c.Limits.OpTurnTimeout }),
}

// intKey builds a whitelist entry for an int field addressed via field(c). The
// scalar is parsed as a base-10 int; the whole config is re-validated before
// persisting, so a range violation (e.g. workers outside [2,4]) is rejected there.
func intKey(name string, field func(*config.Config) *int) configKey {
	return configKey{
		name: name,
		get:  func(c config.Config) string { return strconv.Itoa(*field(&c)) },
		set: func(c *config.Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("%s %q is not a number", name, v)
			}
			*field(c) = n
			return nil
		},
		validate: validateLimits,
	}
}

// int64Key builds a whitelist entry for an int64 (byte-count) field addressed via
// field(c), mirroring intKey.
func int64Key(name string, field func(*config.Config) *int64) configKey {
	return configKey{
		name: name,
		get:  func(c config.Config) string { return strconv.FormatInt(*field(&c), 10) },
		set: func(c *config.Config, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("%s %q is not a number", name, v)
			}
			*field(c) = n
			return nil
		},
		validate: validateLimits,
	}
}

// lookupConfigKey returns the whitelisted key descriptor for name, or false.
func lookupConfigKey(name string) (configKey, bool) {
	for _, k := range configKeys {
		if k.name == name {
			return k, true
		}
	}
	return configKey{}, false
}

// configHelp is the `config` help page. `config` routes on a positional action
// rather than flags, so writeHelp is called with a nil FlagSet and the Flags
// block is omitted; the settable keys are appended by configHelpPage below,
// which reads them off the same table `config set` validates against.
var configHelp = helpSpec{
	name:    "config",
	summary: "get or set configuration values",
	usage: []string{
		"synosync config get [key]",
		"synosync config set <key> <value>",
	},
	describe: `Reads and writes the settings stored in state.db. There is no config file to
edit.

` + "`config get`" + ` with no key prints every settable value as key=value.
` + "`config set`" + ` validates the change before storing it, so an unknown key or an
out-of-range value is refused rather than persisted.

` + "`config set`" + ` needs an idle store, so stop the daemon first. ` + "`config get`" + ` works
either way: while the daemon holds the store it reads the settings from the
daemon instead.

Changing server.host, server.port or server.user on a paired install means the
stored credentials no longer match, so it warns that ` + "`synosync pair`" + ` must be
run again before the daemon will start.`,
	examples: []string{
		"synosync config get",
		"synosync config get log_level",
		"synosync config set log_level debug",
		"synosync config set limits.workers 4",
	},
}

// configHelpPage renders configHelp plus the settable keys. The key list comes
// from configKeyNames, the same table `config set` validates against, so it
// cannot list a key the command would then reject.
func configHelpPage(w io.Writer) {
	writeHelp(w, configHelp, nil)
	fmt.Fprintf(w, "\nKeys:\n")
	for _, l := range wrap(configKeyNames(), helpWidth-2) {
		fmt.Fprintf(w, "  %s\n", l)
	}
}

// cmdConfig implements the `config` subcommand.
func cmdConfig(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "synosync: config needs an action")
		writeUsageHint(stderr, configHelp)
		return 2
	}
	switch {
	case isHelpArg(args[0]):
		configHelpPage(stdout)
		return 0
	case args[0] == "get":
		return configGet(args[1:], stdout, stderr)
	case args[0] == "set":
		return configSet(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "synosync: unknown config action %q, want \"get\" or \"set\"\n", args[0])
		writeUsageHint(stderr, configHelp)
		return 2
	}
}

// openConfigForCommand opens the state database and loads the settings blob,
// printing the friendly locked-database hint when the daemon holds the lock. It
// returns the open store (which the caller must Close) and the loaded config.
// It is the `config set` path; `config get` goes through loadConfigForGet,
// which can fall back to the running daemon instead of insisting on the lock.
func openConfigForCommand(stderr io.Writer) (*store.Store, config.Config, int) {
	st, err := openConfigStore()
	if err != nil {
		if errors.Is(err, store.ErrLocked) {
			fmt.Fprintln(stderr, lockedStoreMessage)
			return nil, config.Config{}, 1
		}
		fmt.Fprintf(stderr, "synosync: open state db: %s\n", userErr(err))
		return nil, config.Config{}, 1
	}
	cfg, err := loadConfigFromStore(st)
	if err != nil {
		_ = st.Close()
		reportConfigLoadError(stderr, err)
		return nil, config.Config{}, 1
	}
	return st, cfg, 0
}

// loadConfigForGet loads the settings blob for the read-only `config get`: from
// the state database when it can be opened, else - when the store is locked,
// normally because the daemon holds bbolt's exclusive lock - live from the
// daemon over its read-only `config` IPC method. It returns
// the config and an exit code (non-zero on failure, with the message already
// printed to stderr); the config is a value copy, so the store is closed before
// returning.
func loadConfigForGet(stderr io.Writer) (config.Config, int) {
	st, err := openConfigStore()
	if err != nil {
		if !errors.Is(err, store.ErrLocked) {
			fmt.Fprintf(stderr, "synosync: open state db: %s\n", userErr(err))
			return config.Config{}, 1
		}
		cfg, ok := configViaDaemon(stderr)
		if !ok {
			return config.Config{}, 1
		}
		return cfg, 0
	}
	defer func() { _ = st.Close() }()
	cfg, err := loadConfigFromStore(st)
	if err != nil {
		reportConfigLoadError(stderr, err)
		return config.Config{}, 1
	}
	return cfg, 0
}

// configViaDaemon fetches the settings blob from the running daemon over the
// read-only `config` IPC method, for a `config get` that found the state
// database locked. When there is no serving daemon behind the lock (a pair or
// login in flight, or a daemon still binding its socket) the stop-the-daemon
// hint stands, so the caller's UX matches the pre-fallback behaviour.
func configViaDaemon(stderr io.Writer) (config.Config, bool) {
	paths, err := resolveDaemonPaths()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return config.Config{}, false
	}
	client, err := ipcDial(paths.DaemonSock())
	if err != nil {
		switch {
		case daemonNotRunning(err):
			fmt.Fprintln(stderr, lockedStoreMessage)
		case daemonBusy(err):
			fmt.Fprintln(stderr, "synosync: daemon is busy or slow to respond - try again")
		default:
			fmt.Fprintf(stderr, "synosync: connect to daemon: %s\n", userErr(err))
		}
		return config.Config{}, false
	}
	defer func() { _ = client.Close() }()

	var rep ipc.ConfigReport
	if err := client.Call("config", nil, &rep); err != nil {
		fmt.Fprintf(stderr, "synosync: query daemon config: %s\n", userErr(err))
		return config.Config{}, false
	}
	return rep.Config, true
}

// configGet handles `config get [key]`.
func configGet(args []string, stdout, stderr io.Writer) int {
	if len(args) > 1 {
		fmt.Fprintln(stderr, "synosync: config get takes at most one key")
		writeUsageHint(stderr, configHelp)
		return 2
	}

	// Resolve the key BEFORE touching the store: a key outside the whitelist is a
	// usage error (exit 2, like a missing/extra argument), not a runtime failure, and
	// naming it must not depend on the state database being openable.
	var k configKey
	if len(args) == 1 {
		var ok bool
		k, ok = lookupConfigKey(args[0])
		if !ok {
			fmt.Fprintf(stderr, "synosync: unknown config key %q\n", args[0])
			fmt.Fprintf(stderr, "usage: synosync config get [key]  (keys: %s)\n", configKeyNames())
			return 2
		}
	}

	cfg, code := loadConfigForGet(stderr)
	if code != 0 {
		return code
	}

	if len(args) == 0 {
		for _, k := range configKeys {
			fmt.Fprintf(stdout, "%s=%s\n", k.name, k.get(cfg))
		}
		return 0
	}

	fmt.Fprintln(stdout, k.get(cfg))
	return 0
}

// configKeyNames lists the settable keys for a usage message, in the stable
// configKeys order.
func configKeyNames() string {
	names := make([]string, 0, len(configKeys))
	for _, k := range configKeys {
		names = append(names, k.name)
	}
	return strings.Join(names, ", ")
}

// configSet handles `config set <key> <value>`.
func configSet(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, "synosync: config set takes a key and a value")
		writeUsageHint(stderr, configHelp)
		return 2
	}
	name, value := args[0], args[1]

	k, ok := lookupConfigKey(name)
	if !ok {
		fmt.Fprintf(stderr, "synosync: unknown config key %q\n", name)
		fmt.Fprintf(stderr, "usage: synosync config set <key> <value>  (keys: %s)\n", configKeyNames())
		return 2
	}

	st, cfg, code := openConfigForCommand(stderr)
	if code != 0 {
		return code
	}
	defer func() { _ = st.Close() }()

	// Capture the pre-change server identity so a server.host/port/user change can
	// erase the now-orphaned keychain entries after the new blob is persisted.
	oldServer := serverAddr(cfg)
	oldUser := cfg.Server.User

	if name == "views.selected" {
		return setSelectedViews(st, cfg, value, stdout, stderr)
	}
	if err := k.set(&cfg, value); err != nil {
		// No "invalid value:" wrapper: the setter's own message already names the key and what
		// is wrong with the value, so the prefix only made this one class of failure read
		// differently from every other.
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	// Validate the field actually being set first, so a bad value reports that
	// field (e.g. "invalid log_level") rather than the first unrelated
	// cross-field error from ValidatePartial.
	if k.validate != nil {
		if err := k.validate(cfg); err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 1
		}
	}
	// The persistence gate is the PARTIAL validation: it
	// enforces every cross-field invariant (chunk size vs resume threshold,
	// keychain-safe host/user, port range) but tolerates a still-incomplete
	// host/user/sync_root trio - the legitimate pre-pairing state pair reads
	// and completes. The whole-config Validate would deadlock a fresh install:
	// setting any one required field always failed on ANOTHER still-empty one,
	// in every call order, and blocked unrelated keys like log_level too.
	// pair/login/daemon still gate on the full Validate.
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	fmt.Fprintf(stdout, "%s=%s\n", name, k.get(cfg))
	warnServerIdentityChange(name, oldServer, oldUser, cfg, stderr)
	cleanupOrphanedServerSecrets(name, oldServer, oldUser, cfg, stderr)
	warnSyncRootChange(name, cfg, stderr)
	return 0
}

// serverIdentityKeys are the connection-identity fields the keychain pin and
// session are addressed by (server address host:port, account user). Changing any
// of them after pairing orphans those keychain entries - the daemon then can't
// find a pin/session for the new identity and won't start without re-pairing.
var serverIdentityKeys = map[string]bool{
	"server.host": true,
	"server.port": true,
	"server.user": true,
}

// warnServerIdentityChange prints a one-line warning when a connection-identity
// field is ACTUALLY changed on an already-paired install. It is the companion of
// cleanupOrphanedServerSecrets (which runs right after): the old identity's
// keychain secrets are cleared automatically best-effort, so the warning no
// longer claims they are left orphaned - it tells the operator the actionable
// fact, that the new identity is unpaired and the daemon will not start until it
// is re-paired with `synosync pair`. It is gated on the SAME paired-and-changed
// condition as cleanupOrphanedServerSecrets (including the unchanged-value
// no-op), so a `config set server.host <current-host>` - which orphans nothing
// and clears nothing - no longer emits a warning falsely claiming both.
func warnServerIdentityChange(name, oldServer, oldUser string, cfg config.Config, stderr io.Writer) {
	if !serverIdentityKeys[name] {
		return
	}
	if cfg.DeviceUUID == "" || cfg.Server.DsID == "" {
		return // not paired yet - nothing to orphan
	}
	if oldServer == serverAddr(cfg) && oldUser == cfg.Server.User {
		return // value unchanged (e.g. set to the same host) - nothing orphaned
	}
	fmt.Fprintln(stderr, "synosync: warning: the server identity changed; synosync clears the "+
		"previous identity's orphaned keychain secrets automatically, but the new identity is not "+
		"paired - the daemon will not start until you re-pair with `synosync pair`")
}

// cleanupOrphanedServerSecrets best-effort erases the keychain entries the OLD
// server identity was keyed under after a `config set server.host/port/user`
// changed it on a paired install. Without this the session
// token and certificate pin the previous (host:port, user) pointed at linger in
// the system keychain forever: a later `unlink` cleans only the CURRENT identity
// (serverAddr of the new blob), so it would report full cleanup while the stale
// secrets survived unreachably. Decision (plan default, over storing a
// pairing-time identity in the blob): clean at set time - a new persistent field
// would bump store.CurrentSchemaVersion per the store-2 rule, and the right to
// delete is unambiguous here (the operator is changing identity, and the change
// is already loud via warnServerIdentityChange).
//
// It runs only AFTER the new blob is persisted, so a failed PutConfig never
// deletes the secrets the still-current config depends on, and it is best effort:
// a keyring backend failure warns (via clearSecret) but never fails the set - the
// value is already saved. The session/password are keyed by (server, user) so
// they are orphaned whenever the old pair differs from the new; the certificate
// pin is keyed by the server address alone, so it is orphaned only when the host
// or port changed, NOT on a user-only change (the pin still serves the same
// server the new user connects to).
func cleanupOrphanedServerSecrets(name, oldServer, oldUser string, newCfg config.Config, stderr io.Writer) {
	if !serverIdentityKeys[name] {
		return
	}
	if newCfg.DeviceUUID == "" || newCfg.Server.DsID == "" {
		return // not paired yet - no secrets were stored under the old identity
	}
	cleanupIdentitySecrets(oldServer, oldUser, serverAddr(newCfg), newCfg.Server.User, stderr)
}

// cleanupIdentitySecrets best-effort erases the keychain entries the OLD server
// identity (oldServer, oldUser) was keyed under once a paired install's connection
// identity has changed to (newServer, newUser). It is the shared core of the two
// identity-change paths - `config set server.*` (cleanupOrphanedServerSecrets) and
// `pair` re-pairing to a new identity (cleanupPairOrphanedSecrets)
// - so both erase an identical set: the (server, user)-keyed session and
// password whenever the pair changed, and the server-address-keyed certificate pin
// only when the host or port changed (a user-only change keeps the pin, which
// still serves the same server). Callers MUST run it only AFTER the new blob is
// persisted, so a failed write never deletes secrets the still-current config
// depends on; it is best effort - a keyring backend failure warns (via
// clearSecret) but never fails the caller.
func cleanupIdentitySecrets(oldServer, oldUser, newServer, newUser string, stderr io.Writer) {
	if oldServer == newServer && oldUser == newUser {
		return // value unchanged (e.g. re-paired to the same identity) - nothing orphaned
	}
	kr := openKeyring()
	_ = clearSecret(stderr, "orphaned session", kr.DeleteSession(oldServer, oldUser))
	_ = clearSecret(stderr, "orphaned password", kr.DeletePassword(oldServer, oldUser))
	if oldServer != newServer {
		_ = clearSecret(stderr, "orphaned certificate pin", kr.DeletePin(oldServer))
	}
}

// warnSyncRootChange prints a one-line warning when sync_root is changed on an
// already-paired install. The state database's baselines describe
// the synced tree under the PREVIOUS root: if the files are not physically moved to
// the new root before the next daemon start, the first local scan reads every tracked
// file as locally deleted and pushes a mass server-side removal.
func warnSyncRootChange(name string, cfg config.Config, stderr io.Writer) {
	if name != "sync_root" {
		return
	}
	if cfg.DeviceUUID == "" || cfg.Server.DsID == "" {
		return // not paired yet - no synced state to diverge from
	}
	fmt.Fprintln(stderr, "synosync: warning: the state database still describes the synced files under "+
		"the previous sync root; move them into the new sync root before the next daemon start, or the "+
		"first scan will treat every tracked file as locally deleted and remove it from the server")
	warnFlatRootContents(cfg, stderr)
}

// warnFlatRootContents names what a flat install would upload from a new sync
// root.
//
// Under a per-view layout the sync root is only a container and a stray file
// beside the view directories is out of scope. Flattened, the sync root IS the
// view, so retargeting one at a populated directory uploads its contents - into
// a shared folder, if that is the selected view. It warns rather than refuses:
// the remedy warnSyncRootChange documents is to MOVE the tree into the new root,
// which makes it legitimately non-empty.
func warnFlatRootContents(cfg config.Config, stderr io.Writer) {
	if !cfg.Views.Flat {
		return
	}
	entries, err := os.ReadDir(cfg.SyncRoot)
	if err != nil || len(entries) == 0 {
		return
	}
	shown := make([]string, 0, 5)
	for _, e := range entries {
		if len(shown) == cap(shown) {
			shown = append(shown, fmt.Sprintf("… and %d more", len(entries)-len(shown)))
			break
		}
		shown = append(shown, e.Name())
	}
	fmt.Fprintf(stderr, "synosync: warning: this install syncs one view directly in the sync root, and "+
		"%s already holds %s - anything there that is not the synced tree will be UPLOADED on the "+
		"first cycle\n", cfg.SyncRoot, strings.Join(shown, ", "))
}

// setSelectedViews is the `config set views.selected` path.
//
// It resolves names against the views the store knows - the daemon records them
// on every refresh - because the selection is matched by the server's view id,
// not by the name. Matching by name made a server-side rename permanently
// suppress the view: the engine adopts the new name into its store record, the
// config still says the old one, and the daemon silently syncs nothing.
func setSelectedViews(st *store.Store, cfg config.Config, value string, stdout, stderr io.Writer) int {
	known, err := st.ListViews()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: list known views: %s\n", userErr(err))
		return 1
	}
	views := make([]config.SelectedView, 0, len(known))
	for _, v := range known {
		if v.Name != "" {
			views = append(views, config.SelectedView{ID: v.ViewID, Name: normpath.Normalize(v.Name)})
		}
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	next := config.Views{}
	if trimmed := strings.TrimSpace(value); trimmed != "" && !strings.EqualFold(trimmed, allViews) {
		if len(views) == 0 {
			fmt.Fprintln(stderr, "synosync: no views are known yet; run the daemon once so it can enumerate them,")
			fmt.Fprintln(stderr, "  or pass the selection to `synosync pair --views`")
			return 1
		}
		selected, err := matchRequested(value, views)
		if err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 1
		}
		next = deriveLayout(selected, views)
	}
	// The exclusions are not part of the selection: changing which views sync keeps them.
	next.Excluded = cfg.Views.Excluded

	if err := next.Validate(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := guardViewChange(st, cfg.Views, next, cfg.SyncRoot, stderr); err != nil {
		if !errors.Is(err, errLayoutLocked) {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		}
		return 1
	}
	cfg.Views = next
	if err := cfg.ValidatePartial(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	if err := st.PutConfig(cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}
	if len(next.Selected) == 0 {
		fmt.Fprintf(stdout, "views.selected=%s\n", allViews)
	} else {
		fmt.Fprintf(stdout, "views.selected=%s\n", strings.Join(next.Names(), ","))
	}
	fmt.Fprintf(stdout, "layout: %s\n", describeIdentity(next))
	return 0
}
