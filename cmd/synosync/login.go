package main

// The `login` subcommand re-authenticates an already-paired server without a
// full re-pair when possible. It reuses the saved device_uuid, ds_id and pinned
// certificate: it connects verifying the pin, then presents the stored session
// token (mode A). If the server reports the session has vanished (8193), it
// prompts for the password and an optional OTP and re-pairs (mode C) over the
// same pinned connection, refreshing the stored session.

import (
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// loginHelp is the `login` help page. The flag list is read off the FlagSet in the
// command below, so only the prose lives here.
var loginHelp = helpSpec{
	name:    "login",
	summary: "re-authenticate with a paired server",
	usage:   []string{"synosync login [flags]"},
	describe: `Re-authenticates with the server this device is already paired to, reusing the
saved device id. It tries session reuse first and falls back to a full password
prompt when the server reports the session is gone.

Every connection checks the server's leaf certificate against the pin captured
at pairing time, so a swapped certificate is refused rather than trusted. A
genuine key change reports both fingerprints and asks you to re-run ` + "`" + `pair` + "`" + `.

Run ` + "`" + `pair` + "`" + ` first: without a stored device id and pin there is nothing to log in
with.`,
	examples: []string{
		"synosync login",
		"synosync login --user bob        # log in as a different account on the same server",
	},
	hint: func(arg string) string {
		return fmt.Sprintf("synosync login --host %s", arg)
	},
}

// cmdLogin implements the `login` subcommand.
func cmdLogin(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	host := fs.String("host", "", "server `host` (overrides config)")
	port := fs.Int("port", 0, "server `port` (overrides config)")
	user := fs.String("user", "", "account `name` (overrides config)")
	if code, ok := parseFlags(fs, loginHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(loginHelp, fs.Args(), stderr); !ok {
		return code
	}

	st, err := openConfigStore()
	if err != nil {
		if errors.Is(err, store.ErrLocked) {
			fmt.Fprintln(stderr, lockedStoreMessage)
			return 1
		}
		fmt.Fprintf(stderr, "synosync: open state db: %s\n", userErr(err))
		return 1
	}
	defer func() { _ = st.Close() }()

	cfg, err := loadConfigFromStore(st)
	if err != nil {
		reportConfigLoadError(stderr, err)
		return 1
	}
	applyServerFlags(&cfg, *host, *port, *user)
	// Validate the whole config once the flags are folded in, mirroring `pair`
	// (pair.go) and `daemon`: the pre-flight checks in runLogin only assert the
	// paired fields are non-empty, not port range / log level / limits, so an
	// out-of-range --port would otherwise slip through to a murky connect failure.
	// login never persists the config, so this is defense-in-depth/consistency.
	//
	// Gate the validation on the install actually being paired: a fresh/unpaired
	// store has no config, so loadConfigFromStore returns config.Default() with an
	// empty sync_root, and an unconditional Validate would report "sync_root must
	// be set" (exit 2) instead of runLogin's friendly "not paired yet; run
	// `synosync pair` first" (exit 1) - and would reclassify a never-configured
	// install as a usage error. The whole-config check is meant to reject a bad
	// CLI flag on an already-paired install, so only run it there; the not-paired
	// case is left to runLogin's preflight.
	if loginPaired(cfg) {
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 2
		}
	}
	deps := authDeps{
		store:  st,
		kr:     openKeyring(),
		prompt: newPrompter(stdinFile, stderr),
		client: clientInfo(),
	}
	return runLogin(deps, cfg, stdout, stderr)
}

// loginPaired reports whether cfg carries the coordinates a prior `synosync
// pair` persists - the stable device id, the learned server serial, and the
// server host/user. login uses it both to gate whole-config validation (so an
// unpaired install gets the friendly not-paired message rather than a sync_root
// validation error) and as runLogin's preflight, keeping the two checks in sync.
func loginPaired(cfg config.Config) bool {
	return cfg.DeviceUUID != "" && cfg.Server.DsID != "" &&
		cfg.Server.Host != "" && cfg.Server.User != ""
}

// runLogin is the testable core of the `login` subcommand.
func runLogin(d authDeps, cfg config.Config, stdout, stderr io.Writer) int {
	if !loginPaired(cfg) {
		fmt.Fprintln(stderr, "synosync: not paired yet; run `synosync pair` first")
		return 1
	}

	ctx, cancel := authContext()
	defer cancel()

	addr := serverAddr(cfg)
	server := addr

	pin, err := d.kr.Pin(server)
	if err != nil {
		if errors.Is(err, secrets.ErrNotFound) {
			fmt.Fprintln(stderr, "synosync: no pinned certificate found; run `synosync pair` first")
			return 1
		}
		printKeyringErr(stderr, "read cert pin", err)
		return 1
	}
	// A present-but-empty pin must never reach transport.Connect: an empty Pin
	// there selects capture mode (trust any leaf), silently defeating pinning on
	// this verify-only path. Treat a blank stored pin as a re-pair signal.
	if pin == "" {
		fmt.Fprintln(stderr, "synosync: stored certificate pin is empty; re-pair with `synosync pair`")
		return 1
	}
	session, err := d.kr.Session(server, cfg.Server.User)
	if err != nil && !errors.Is(err, secrets.ErrNotFound) {
		printKeyringErr(stderr, "read session", err)
		return 1
	}

	// Pin set → verify mode: the leaf must match the one captured at pairing.
	sess, err := transport.Connect(ctx, transport.ConnectConfig{
		Addr:  addr,
		Agent: d.client.Agent(cfg.DeviceUUID),
		DsID:  cfg.Server.DsID,
		Pin:   pin,
	})
	if err != nil {
		if errors.Is(err, transport.ErrPinMismatch) {
			// The wrapped error carries the got/want fingerprints; surface them so
			// the user can tell a benign re-issue (same key) from a real key change.
			fmt.Fprintf(stderr, "synosync: server certificate changed (%v); re-pair with `synosync pair`\n", err)
			return 1
		}
		printConnectErr(stderr, "connect", addr, err)
		return 1
	}
	defer sess.Close()

	// Mode A: present the stored session token, when there is one.
	if session != "" {
		err = auth.Reuse(ctx, sess.Conn(), session, d.client.Agent(cfg.DeviceUUID), cfg.Server.DsID)
		if err == nil {
			migrateLegacyPin(d, server, pin, sess, stdout, stderr)
			fmt.Fprintf(stdout, "session for %s is still valid\n", addr)
			return 0
		}
		if !errors.Is(err, auth.ErrSessionVanished) {
			fmt.Fprintf(stderr, "synosync: %s\n", pairErrorMessage(err))
			return 1
		}
		fmt.Fprintln(stdout, "session expired; re-authenticating")
	}

	// Mode C over the same pinned connection.
	creds, err := promptCredentials(d.prompt, cfg.Server.User)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	newSession, err := pairWithOTPRetry(sess.Conn(), d.client, cfg.DeviceUUID, cfg.Server.DsID, creds, d.prompt)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", pairErrorMessage(err))
		return 1
	}
	if err := d.kr.SetSession(server, cfg.Server.User, newSession); err != nil {
		printKeyringErr(stderr, "store session", err)
		return 1
	}

	// The re-pair ran over the same pinned connection (transport.Connect verified
	// the stored pin), so upgrade a stored legacy full-DER pin to the SPKI form
	// here too - otherwise the pin stays legacy until the daemon's first reuse
	// migrates it. A no-op for an already-SPKI pin.
	migrateLegacyPin(d, server, pin, sess, stdout, stderr)

	fmt.Fprintf(stdout, "re-authenticated with %s as %s\n", addr, cfg.Server.User)
	return 0
}

// migrateLegacyPin transparently upgrades a stored legacy full-DER pin to the
// SPKI form once the same certificate has verified on a successful reuse: a
// routine re-issue with the same keypair then no longer breaks the sync. It is a
// best-effort, no-op when the stored pin is already SPKI or the presented leaf's
// SPKI fingerprint is unavailable; a store failure is reported but not fatal
// (the legacy pin keeps verifying until the next attempt).
func migrateLegacyPin(d authDeps, server, pin string, sess *transport.Session, stdout, stderr io.Writer) {
	if !transport.IsLegacyPin(pin) {
		return
	}
	spki := sess.SPKIPin()
	if spki == "" {
		return
	}
	if err := d.kr.SetPin(server, spki); err != nil {
		printKeyringErr(stderr, "upgrade cert pin", err)
		return
	}
	fmt.Fprintln(stdout, "certificate pin upgraded to SPKI")
}
