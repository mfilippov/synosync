package main

// The `unlink` subcommand undoes a pairing: it detaches this device on the server
// (best effort - a server that is unreachable only earns a warning, the local
// cleanup still runs), then erases the locally held credentials (session token,
// account password and pinned certificate in the system keychain) and all durable
// local state (the settings blob plus the views/baselines/queue/conflicts in
// state.db). After unlink the install is fully unpaired, so `pair` would start
// fresh. It requires the daemon to be stopped: the store is opened exclusively, so
// a running daemon surfaces the same friendly "stop the daemon" hint as
// pair/login. Side effects route through the shared injection vars (store,
// keyring) plus a serverUnlink seam, so the flow is unit-tested without a live
// server.
//
// Corrupt-config tolerance: a bit-rotted settings blob makes the
// strict GetConfig fail, which dead-ends pair/login/config/daemon - all of which
// need the whole blob. unlink is the intended recovery path (unlink → re-pair),
// so it must NOT dead-end on the same blob: on a corrupt config it best-effort
// parses just the keychain-cleanup coordinates (server host/port/user) with a
// lenient decode, skips the server detach (the coordinates/session it needs may
// be exactly what's undecodable), and still clears the keychain and resets the
// store so a fresh `pair` can follow.
//
// Unopenable-store recovery: when the corruption is so deep that
// store.Open itself fails with ErrCorruptRecord (a lost/truncated/zeroed
// schema_version marker), even that flow is unreachable - so unlink falls back
// to a confirmed, destructive deletion of state.db (recoverCorruptStore). The
// keychain cleanup is skipped there (its keys live inside the unreadable
// database) with a warning; any other Open failure stays non-destructive.

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/daemon"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/store"
)

// serverUnlink performs the best-effort server-side device detach: it builds the
// keychain-backed connector (verify-pin dial + mode-A session reuse, like the
// daemon), opens one authenticated session and calls UnlinkConnection. It is an
// injection point so unlink tests cover the happy path and the unreachable-server
// path without a live Synology server.
var serverUnlink = func(ctx context.Context, cfg config.Config, kr secretStore, client auth.ClientInfo) error {
	connector, session, err := daemon.NewConnector(cfg, kr, client, nil)
	if err != nil {
		return err
	}
	sess, err := connector(ctx)
	if err != nil {
		return err
	}
	defer sess.Close()
	return actions.NewClientWithIdentity(sess, session,
		clientIdentity(client, cfg.DeviceUUID, cfg.Server.DsID), nil).UnlinkConnection(ctx)
}

// recoverUnlinkCoordinates best-effort parses just the server coordinates
// (host/port/user - the keys the keychain secrets are stored under) from the raw,
// undecodable settings blob, using a lenient decode (plain json.Unmarshal, no
// DisallowUnknownFields, so an unknown-field or partially-garbled blob still
// yields whatever parses). It never fails: a field that doesn't parse is left at
// its default, and the returned config is only ever used to key the keychain
// cleanup, so a partial recovery still clears whatever it can before Reset.
func recoverUnlinkCoordinates(st *store.Store, stderr io.Writer) config.Config {
	cfg := config.Default()
	blob, err := st.GetConfigBlob()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: warning: could not read raw settings (%v); keychain cleanup may be incomplete\n", err)
		return cfg
	}
	var partial struct {
		Server struct {
			Host string `json:"host"`
			Port int    `json:"port"`
			User string `json:"user"`
		} `json:"server"`
	}
	if err := json.Unmarshal(blob, &partial); err != nil {
		fmt.Fprintf(stderr, "synosync: warning: could not recover server coordinates (%v); keychain cleanup may be incomplete\n", err)
		return cfg
	}
	if partial.Server.Host != "" {
		cfg.Server.Host = partial.Server.Host
	}
	if partial.Server.Port != 0 {
		cfg.Server.Port = partial.Server.Port
	}
	if partial.Server.User != "" {
		cfg.Server.User = partial.Server.User
	}
	return cfg
}

// cleanupDaemonArtifacts best-effort removes the transient daemon artifacts that
// should not outlive a pairing holding a trace of it - currently the Windows
// named-pipe secret next to state.db (never created on Unix, so os.Remove there
// is a harmless ErrNotExist no-op). The single-instance lock file and the
// rotating log files are intentionally KEPT: the lock is recreated harmlessly on
// the next daemon start, and the log history is useful for post-mortem
// diagnostics - so unlink names where they remain rather than deleting them.
// Everything here is best effort: a path-resolution or removal failure prints a
// warning but never blocks the (already completed) local cleanup. It is an
// injection seam so unlink tests exercise it against a temp state dir.
var cleanupDaemonArtifacts = func(stdout, stderr io.Writer) {
	paths, err := resolveDaemonPaths()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: warning: could not resolve daemon paths for cleanup: %s\n", userErr(err))
		return
	}
	if err := os.Remove(paths.PipeSecret()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(stderr, "synosync: warning: could not remove pipe secret: %s\n", userErr(err))
	}
	fmt.Fprintf(stdout, "log files kept at %s\n", paths.LogFile())
}

// unlinkHelp is the `unlink` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var unlinkHelp = helpSpec{
	name:    "unlink",
	summary: "unlink the device and clear credentials",
	usage:   []string{"synosync unlink [flags]"},
	describe: `Detaches this device from the server and wipes the local pairing: it clears the
session token and the certificate pin from the system keychain and resets
state.db. The account password is never stored, so there is none to clear.

It asks for confirmation first and needs a stopped daemon. If the server cannot
be reached it warns and still performs the local cleanup.

Local files in the sync root are never touched.`,
	examples: []string{
		"synosync unlink",
		"synosync unlink --yes    # no prompt, for scripts",
	},
}

// cmdUnlink implements the `unlink` subcommand.
func cmdUnlink(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("unlink", flag.ContinueOnError)
	yes := fs.Bool("yes", false, "skip the confirmation prompt (for non-interactive use)")
	if code, ok := parseFlags(fs, unlinkHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(unlinkHelp, fs.Args(), stderr); !ok {
		return code
	}

	st, err := openConfigStore()
	if err != nil {
		if errors.Is(err, store.ErrLocked) {
			fmt.Fprintln(stderr, lockedStoreMessage)
			return 1
		}
		// A store so corrupt that Open itself fails (a lost/truncated/zeroed
		// schema_version marker) dead-ends the normal flow below - the Reset it
		// would recover with needs a successful Open. Without this branch the
		// `synosync unlink` hint inside the Open error would point back at this
		// very failure, a circular dead-end only a manual state.db deletion
		// cured. Any other Open failure (transient I/O,
		// ErrSchemaTooNew) keeps the non-destructive report-and-exit.
		if errors.Is(err, store.ErrCorruptRecord) {
			return recoverCorruptStore(*yes, stdout, stderr, err)
		}
		fmt.Fprintf(stderr, "synosync: open state db: %s\n", userErr(err))
		return 1
	}
	defer func() { _ = st.Close() }()

	cfg, err := loadConfigFromStore(st)
	corrupt := false
	if err != nil {
		// A transient backend read failure (not corruption) must not wipe state:
		// abort so a retry can succeed once the store is readable again.
		if !errors.Is(err, store.ErrCorruptRecord) {
			fmt.Fprintf(stderr, "synosync: load config: %s\n", userErr(err))
			return 1
		}
		// A corrupt blob is the case unlink exists to recover from: best-effort
		// parse just the keychain-cleanup coordinates and proceed to Reset.
		fmt.Fprintf(stderr, "synosync: warning: stored settings are corrupt (%v); recovering keychain coordinates best-effort and clearing local state\n", err)
		cfg = recoverUnlinkCoordinates(st, stderr)
		corrupt = true
	}
	// The not-paired guard is skipped for a corrupt blob: DeviceUUID/DsID may be
	// exactly what failed to decode, and the user wants a clean local slate.
	if !corrupt && cfg.DeviceUUID == "" && cfg.Server.DsID == "" {
		// Exit 0, on stdout: the end state the caller asked for is already in place. `unlink`
		// is what a teardown script runs to reach "this machine is not paired", and reporting
		// failure for having arrived there means every such script has to special-case it, or
		// stop at the first re-run. Nothing is wrong here and nothing is left to do.
		fmt.Fprintln(stdout, "Not paired; nothing to unlink.")
		return 0
	}

	server := serverAddr(cfg)

	if !*yes {
		ok, code := confirmUnlink(server, stdout, stderr)
		if !ok {
			return code
		}
	}

	kr := openKeyring()

	// Best effort: a reachable server is told to forget this device, but an
	// unreachable one (or a stale session) must not block the local cleanup -
	// otherwise the credentials would be stuck on a box the user can't reach. On a
	// corrupt config the detach is skipped: building the connector needs a whole,
	// decodable config (host/pin/session), which is precisely what's undecodable.
	if !corrupt {
		ctx, cancel := authContext()
		defer cancel()
		if err := serverUnlink(ctx, cfg, kr, clientInfo()); err != nil {
			fmt.Fprintf(stderr, "synosync: warning: could not notify the server (continuing with local cleanup): %s\n", userErr(err))
		}
	}

	// Erase the locally held secrets. A never-stored item (ErrNotFound) is already
	// clear, so only a real backend failure is reported. All three deletes are
	// attempted so as much as possible is cleared even if one fails.
	secretFailed := clearSecret(stderr, "session", kr.DeleteSession(server, cfg.Server.User))
	secretFailed = clearSecret(stderr, "password", kr.DeletePassword(server, cfg.Server.User)) || secretFailed
	secretFailed = clearSecret(stderr, "certificate pin", kr.DeletePin(server)) || secretFailed

	// A keychain backend failure must NOT be followed by wiping the store: the
	// keychain entries are keyed by server/user, so erasing the config that holds
	// them would orphan the still-present credentials beyond the reach of a retry
	// (`unlink` would then report "not paired"). Preserve local state so the user
	// can re-run `unlink` once the keychain is reachable; the flow is idempotent.
	if secretFailed {
		fmt.Fprintln(stderr, "synosync: keychain cleanup incomplete; local state left intact - re-run `synosync unlink` once the keychain is reachable")
		return 1
	}

	// Drop all durable local state, returning the database to the unpaired state.
	if err := st.Reset(); err != nil {
		fmt.Fprintf(stderr, "synosync: clear local state: %s\n", userErr(err))
		return 1
	}

	// Remove the transient pipe secret and note the intentionally-kept artifacts
	// (lock file, log history). Best effort - never fails the unlink.
	cleanupDaemonArtifacts(stdout, stderr)

	fmt.Fprintf(stdout, "unlinked from %s; local credentials and state cleared\n", server)
	return 0
}

// recoverCorruptStore is the destructive no-Open recovery path for a state
// database store.Open itself rejects as corrupt (ErrCorruptRecord - a
// lost/truncated/zeroed schema_version marker, or bbolt refusing the file with
// torn meta pages). The database is unreadable, so
// the server coordinates the keychain secrets are keyed under cannot be
// recovered: the keychain cleanup is skipped with a warning (re-pairing against
// the same server overwrites the entries), and recovery is reduced to deleting
// state.db outright - the only cure - plus the same best-effort pipe-secret
// cleanup as a normal unlink. Deletion is confirmed unless --yes was given.
func recoverCorruptStore(yes bool, stdout, stderr io.Writer, openErr error) int {
	fmt.Fprintf(stderr, "synosync: the state database is corrupt beyond repair: %v\n", openErr)
	fmt.Fprintln(stderr, "synosync: warning: the server coordinates inside it are unreadable, so the keychain entries cannot be cleared; re-pairing against the same server will overwrite them")
	if !yes {
		ok, code := confirmDeleteCorruptStore(stdout, stderr)
		if !ok {
			return code
		}
	}
	paths, err := resolveDaemonPaths()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: resolve state paths: %s\n", userErr(err))
		return 1
	}
	if err := os.Remove(paths.StateDB()); err != nil && !errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(stderr, "synosync: remove state db: %s\n", userErr(err))
		return 1
	}
	cleanupDaemonArtifacts(stdout, stderr)
	fmt.Fprintln(stdout, "corrupt state database removed; run `synosync pair` to re-pair")
	return 0
}

// confirmDeleteCorruptStore prompts before the destructive corrupt-store
// recovery, mirroring confirmUnlink's return contract: (true, 0) to proceed,
// (false, 0) on a clean decline, (false, 1) when the prompt could not be read.
func confirmDeleteCorruptStore(stdout, stderr io.Writer) (bool, int) {
	p := newPrompter(stdinFile, stderr)
	ans, err := p.ReadLine("Delete the corrupt state database and erase local sync state? [y/N]: ")
	if err != nil {
		fmt.Fprintf(stderr, "synosync: read confirmation: %s\n", userErr(err))
		return false, 1
	}
	if !isAffirmative(ans) {
		fmt.Fprintln(stdout, "unlink cancelled")
		return false, 0
	}
	return true, 0
}

// confirmUnlink prompts for confirmation. It returns (true, 0) to proceed, and
// (false, code) otherwise - code 0 when the user declined (a clean cancel) and 1
// when the prompt could not be read.
func confirmUnlink(server string, stdout, stderr io.Writer) (bool, int) {
	p := newPrompter(stdinFile, stderr)
	ans, err := p.ReadLine(fmt.Sprintf("Unlink from %s and erase local credentials and state? [y/N]: ", server))
	if err != nil {
		fmt.Fprintf(stderr, "synosync: read confirmation: %s\n", userErr(err))
		return false, 1
	}
	if !isAffirmative(ans) {
		fmt.Fprintln(stdout, "unlink cancelled")
		return false, 0
	}
	return true, 0
}

// isAffirmative reports whether s is a yes answer ("y"/"yes", case-insensitive).
func isAffirmative(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "y", "yes":
		return true
	default:
		return false
	}
}

// clearSecret reports the outcome of one keychain deletion and returns whether a
// real backend failure occurred: a missing item (ErrNotFound) is already clear,
// silent and not a failure; any other error is warned about (so a stuck
// credential is visible) and reported as true so the caller can preserve local
// state for a retry instead of orphaning the surviving credentials.
func clearSecret(stderr io.Writer, label string, err error) bool {
	if err == nil || errors.Is(err, secrets.ErrNotFound) {
		return false
	}
	fmt.Fprintf(stderr, "synosync: warning: could not erase %s: %s\n", label, userErr(err))
	return true
}
