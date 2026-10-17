package main

// The `pair` subcommand performs first-time mode-C pairing with a Synology Drive
// server: it learns the server ds_id (TEST_CONN), brings up the encrypt_channel +
// TLS connection while capturing the leaf certificate pin, then authenticates
// with the account password and an optional one-time password. On success it
// persists the session token and the pinned cert in the system keychain and the
// device_uuid and ds_id in the state database (state.db), so later `login` runs
// need no password.

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// pairHelp is the `pair` help page. The flag list is read off the FlagSet below,
// so only the prose lives here.
var pairHelp = helpSpec{
	name:    "pair",
	summary: "pair with a Synology Drive server",
	usage:   []string{"synosync pair [flags]"},
	describe: `Pairs this device with a Synology Drive server. It probes the server, brings
up the TLS connection while capturing the leaf certificate's SPKI fingerprint,
then authenticates with the account password (read without echo) and a one-time
password if the account has 2FA.

On success the session token and the certificate pin go to the system keychain
and the device id to state.db, so later ` + "`login`" + ` runs need no password. The pinned
fingerprint is printed so you can record it and pass it to --expect-pin next
time.

Before any password is sent, the certificate is also checked against the roots
this computer trusts, for the host name given. A certificate that passes is
reported and pairing goes on. One that does not (a self-signed DSM, a name that
does not match, pairing by IP address) is shown with its key, and pairing asks
whether to trust and pin it; --accept-untrusted answers yes without asking, and
a matching --expect-pin counts as the answer too.

Host, port, user and sync root may come from these flags or from settings
already stored by ` + "`synosync config set`" + `. The flags win. Settings live in
state.db, which the daemon locks, so stop the daemon before pairing.

What lands in the sync root depends on how many views you sync. Sync several and
each gets its own subdirectory; sync exactly one and its contents go DIRECTLY in
the sync root. pair asks when the account has more than one view, listing them by
number; --views answers without the prompt. Selecting a single view needs an
empty sync root on a first setup, because everything already in it would be
uploaded on the first cycle.`,
	examples: []string{
		"synosync pair --host drive.example.com --user alice --sync-root /home/alice/Drive",
		"synosync config set server.host drive.example.com",
		"synosync pair                       # host and user taken from the stored config",
		"synosync pair --host nas --expect-pin spki-sha256:1a2b...  # verified first contact",
		"synosync pair --host 192.168.1.5 --accept-untrusted   # the DSM's own self-signed certificate",
		"synosync pair --host nas --user alice --sync-root ~/Drive --views home",
		"synosync pair --host nas --user alice --sync-root ~/Drive --views all   # no prompt",
	},
	hint: func(arg string) string {
		return fmt.Sprintf("synosync pair --host %s --user <name> --sync-root <path>", arg)
	},
}

// cmdPair implements the `pair` subcommand.
func cmdPair(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pair", flag.ContinueOnError)
	host := fs.String("host", "", "server `host` (overrides config)")
	port := fs.Int("port", 0, "server `port` (overrides config)")
	user := fs.String("user", "", "account `name` (overrides config)")
	syncRoot := fs.String("sync-root", "", "absolute `path` of the local directory to sync (overrides config)")
	expectPin := fs.String("expect-pin", "", "require the server's certificate `pin` to equal this value (TOFU out-of-band check); refuse to pair on mismatch")
	acceptUntrusted := fs.Bool("accept-untrusted", false, "pair even when no root this computer trusts vouches for the server's certificate (a self-signed DSM); the key is still pinned")
	views := fs.String("views", "", "which views to sync: a comma-separated `list` of names or of the numbers pair prints (default: ask, or all when not interactive)")
	if code, ok := parseFlags(fs, pairHelp, args, stdout, stderr); !ok {
		return code
	}
	if code, ok := rejectExtraArgs(pairHelp, fs.Args(), stderr); !ok {
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
	// Capture the connection identity the loaded blob carried BEFORE the flags
	// overlay it, so a re-pair to a NEW identity can erase the OLD identity's
	// now-orphaned keychain secrets after the new blob is persisted.
	// `paired` is true only for an already fully-paired blob
	// (DeviceUUID AND DsID set), so a fresh install cleans nothing.
	prior := priorIdentity{
		paired:   cfg.DeviceUUID != "" && cfg.Server.DsID != "",
		server:   serverAddr(cfg),
		user:     cfg.Server.User,
		syncRoot: cfg.SyncRoot,
	}
	applyServerFlags(&cfg, *host, *port, *user)
	if *syncRoot != "" {
		if !filepath.IsAbs(*syncRoot) {
			fmt.Fprintf(stderr, "synosync: --sync-root %q must be an absolute path\n", *syncRoot)
			return 2
		}
		// Refuse a root holding synosync's own state or logs. With a single selected view the
		// sync root IS the view, so the scanner would walk state.db while bbolt holds it open
		// and upload this device's pairing to the server.
		if paths, perr := resolveDaemonPaths(); perr == nil {
			if err := config.CheckSyncRootLocation(*syncRoot, paths); err != nil {
				fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
				return 2
			}
		}
		cfg.SyncRoot = *syncRoot
	}
	if cfg.Server.Host == "" || cfg.Server.User == "" {
		fmt.Fprintln(stderr, "synosync: pair requires a server host and user (use --host/--user, or pre-set them with `synosync config set server.host <host>` and `config set server.user <user>`)")
		return 2
	}
	if cfg.SyncRoot == "" {
		fmt.Fprintln(stderr, "synosync: pair requires a local sync directory (use --sync-root, or pre-set it with `synosync config set sync_root <abs-path>`)")
		return 2
	}
	// Validate the whole config as soon as the flags are folded in - BEFORE the
	// TEST_CONN probe and the interactive password/OTP exchange - mirroring the
	// login fix: runPair's late Validate (kept as the last
	// guard before the keychain writes) only fires after the server has already
	// registered the device, so failing there (e.g. `--port 70000`) discards a
	// completed pairing and forces the user to repeat the password/OTP. The
	// friendly per-field checks above run first, so an incomplete mid-pairing
	// blob still reports the missing field rather than a generic validation
	// error.
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 2
	}

	deps := authDeps{
		store:  st,
		kr:     openKeyring(),
		prompt: newPrompter(stdinFile, stderr),
		client: clientInfo(),
	}
	return runPair(deps, cfg, prior, *expectPin, *acceptUntrusted, *views, stdout, stderr)
}

// authDeps bundles the side-effecting collaborators of pair/login so the core
// flow is exercised by tests with mocks.
type authDeps struct {
	store  *store.Store
	kr     secretStore
	prompt prompter
	client auth.ClientInfo
}

// priorIdentity records the connection identity a paired install carried BEFORE
// this pair overlaid its flags, so runPair can erase the now-orphaned keychain
// secrets of the OLD identity after a successful re-pair to a NEW one (the
// pair-path mirror of `config set server.*`'s
// cleanupOrphanedServerSecrets). `paired` is true only when
// the loaded blob was already fully paired (DeviceUUID AND DsID set), so a fresh
// install - which stored nothing under an old identity - cleans nothing.
type priorIdentity struct {
	paired bool
	server string
	user   string
	// syncRoot is the sync root the stored blob carried, so a retarget by
	// --sync-root can be reported the way `config set sync_root` reports one.
	syncRoot string
}

// putPairedConfig persists the final settings blob after the server-side device
// registration. It is a package var so a test can simulate a failure of this
// final write - the exact window the up-front device_uuid persist
// closes - and assert a retry reuses the same device_uuid.
var putPairedConfig = func(st *store.Store, cfg config.Config) error {
	return st.PutConfig(cfg)
}

// runPair is the testable core of the `pair` subcommand. It returns a process
// exit code and writes user-facing messages to stdout/stderr.
func runPair(d authDeps, cfg config.Config, prior priorIdentity, expectPin string, acceptUntrusted bool, requestedViews string, stdout, stderr io.Writer) int {
	ctx, cancel := authContext()
	defer cancel()

	addr := serverAddr(cfg)

	reply, err := transport.TestConn(ctx, addr)
	if err != nil {
		printConnectErr(stderr, "probe", addr, err)
		return 1
	}
	// A success probe must carry the ds_id (encrypt_channel restore_id); without
	// it pairing would persist an empty ds_id and the next login would refuse.
	if !reply.Succeeded() || reply.DsID == "" {
		fmt.Fprintf(stderr, "synosync: probe %s: server returned no ds_id (result %d)\n", addr, reply.Result)
		return 1
	}
	dsID := reply.DsID

	deviceUUID := cfg.DeviceUUID
	freshUUID := deviceUUID == ""
	if freshUUID {
		deviceUUID, err = auth.NewDeviceUUID()
		if err != nil {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
			return 1
		}
	}

	// Fold the learned server ds_id and the device identity into the config and
	// validate the whole blob BEFORE connecting and registering. Validating up
	// front (rather than after pairing, the former placement) refuses an
	// invalid config - e.g. an out-of-range port reaching runPair directly -
	// without a wasted connect, an orphaned server-side device registration, or a
	// keychain write. DeviceUUID and DsID are not validated fields, so folding them
	// in cannot invalidate an otherwise-valid config.
	cfg.DeviceUUID = deviceUUID
	cfg.Server.DsID = dsID
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}

	// Empty Pin → capture mode: trust this leaf and record its fingerprint.
	sess, err := transport.Connect(ctx, transport.ConnectConfig{
		Addr:  addr,
		Agent: d.client.Agent(deviceUUID),
		DsID:  dsID,
	})
	if err != nil {
		printConnectErr(stderr, "connect", addr, err)
		return 1
	}
	defer sess.Close()

	// Trust-on-first-use, but let a careful operator verify the presented pin
	// out-of-band: if --expect-pin was given and the leaf the server actually
	// presented does not match it, refuse before prompting for a password or
	// writing anything to the keychain.
	if expectPin != "" && !pinMatches(expectPin, sess.Pin(), sess.DERPin()) {
		// Show the presented pin in the format the operator supplied, so a legacy
		// full-DER --expect-pin is compared against the leaf's DER fingerprint
		// rather than its (always-different) SPKI form.
		presented := sess.Pin()
		if transport.IsLegacyPin(strings.ToLower(strings.TrimSpace(expectPin))) {
			presented = sess.DERPin()
		}
		fmt.Fprintf(stderr, "synosync: certificate pin mismatch: server presented %s, expected %s\n", presented, expectPin)
		return 1
	}

	// A pin is already recorded for this server and the key changed. Re-pairing is the
	// DOCUMENTED remedy for a pin mismatch, so this is the path an operator is sent down by
	// the daemon's own hint after a mismatch - which means it is also the path an attacker who
	// can force one sends them down. Overwriting the pin here without a word made the remedy a
	// silent trust-on-first-use reset, and the password and OTP would then go out over the very
	// session whose identity just changed.
	//
	// SPKI pinning means this fires only on a real key change: an ordinary certificate
	// re-issue with the same keypair (a DSM behind Let's Encrypt, every 90 days) keeps the same
	// pin and never reaches here. A move to a different host or port has no stored pin under
	// its own key and is unaffected too.
	//
	// --expect-pin is the out-of-band confirmation, and it was checked above, so a matched one
	// has already proved the operator verified this key through another channel.
	if expectPin == "" {
		if stored, perr := d.kr.Pin(addr); perr == nil && stored != "" && !pinMatches(stored, sess.Pin(), sess.DERPin()) {
			fmt.Fprintf(stderr, "synosync: the server's certificate key CHANGED for %s\n", addr)
			fmt.Fprintf(stderr, "synosync:   recorded:  %s\n", stored)
			fmt.Fprintf(stderr, "synosync:   presented: %s\n", sess.Pin())
			fmt.Fprintln(stderr, "synosync: this is either a genuine key change or an interception.")
			fmt.Fprintln(stderr, "synosync: verify the presented value out of band, then re-run with:")
			fmt.Fprintf(stderr, "synosync:   synosync pair --expect-pin %s ...\n", sess.Pin())
			return 1
		}
	}

	if !trustServerCertificate(d.prompt, sess, cfg.Server.Host, expectPin != "", acceptUntrusted, stdout, stderr) {
		return 1
	}

	creds, err := promptCredentials(d.prompt, cfg.Server.User)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}

	// Persist a freshly-generated device_uuid BEFORE the irreversible server-side
	// device registration in pairWithOTPRetry below. device_uuid lives only in the
	// config blob; when it was written solely after pairing (its former only
	// place), a failure of that final write lost the in-memory id, so a retry
	// generated a fresh id and registered a SECOND, orphaned device server-side.
	// Writing it up front makes a retry reuse the same id - re-pairing is then
	// idempotent server-side. The pin→session→final-config write order
	// after the turn is unchanged.
	//
	// Persist ONLY the device identity (plus the user-supplied server coordinates
	// already folded into cfg) here - NOT the ds_id. The ds_id is written
	// authoritatively by the final putPairedConfig below, only after the pairing
	// turn succeeds. Persisting it up front too would leave a "paired-looking" blob
	// (device_uuid AND ds_id both set) after a failed turn (wrong password/OTP, a
	// network drop, or Ctrl-C), which every "is paired" predicate - daemon/login/
	// config, all gated on a non-empty ds_id - reads as paired against an empty
	// keychain: `daemon` would then loop on "no pinned certificate" instead of
	// reporting "not paired". Clearing DsID keeps the device_uuid-reuse
	// guarantee intact while the ds_id gate correctly reads "not paired" until
	// pairing actually completes.
	if freshUUID {
		early := cfg
		early.Server.DsID = ""
		if err := d.store.PutConfig(early); err != nil {
			fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
			return 1
		}
	}

	session, err := pairWithOTPRetry(sess.Conn(), d.client, deviceUUID, dsID, creds, d.prompt)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", pairErrorMessage(err))
		return 1
	}

	// Choose the views before ANY write. The selection decides where every path
	// resolves, so it has to be settled while the sync root is still untouched -
	// and it needs an authenticated session, which is why it cannot happen sooner.
	// `fresh` is what makes the empty-sync-root guard apply to a first setup only:
	// a re-pair to a new server runs against a root legitimately full of files.
	// Established means "files are already on disk under the stored layout", which
	// is what makes re-deciding a relocation. HasBaselines is the direct question;
	// a paired-but-never-synced install has nothing to move and may still choose.
	established, err := d.store.HasBaselines()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: cannot tell whether anything is synced yet: %s\n", userErr(err))
		return 1
	}
	lister := actions.NewClientWithIdentity(sess, session, clientIdentity(d.client, deviceUUID, dsID), nil)
	// A FRESH budget for the view listing, for exactly the reason pairOnce takes one.
	// The context opened at the top of this function is a wall-clock bound, and by the
	// time the listing runs it has already covered the TEST_CONN probe, the TLS
	// handshake, an interactive password prompt and the server's own authentication
	// turn. What is left of it is not a network budget, it is a remainder - and once
	// that remainder reached zero, netctx.Watch stamped the already-expired deadline
	// straight onto the socket, so the very FIRST write of query_user_info failed
	// instantly and poisoned the connection (ErrConnBroken) with a server and an
	// operator that had both done nothing wrong. Nothing bounds the earlier steps
	// tightly enough to make the remainder predictable, so the listing must not
	// inherit it.
	//
	// selectViews prompts for a choice AFTER this turn, which is safe: the context it
	// takes bounds only the QueryUserInfo call it opens with, and nothing past the
	// prompt uses it.
	viewsCtx, viewsCancel := authContext()
	defer viewsCancel()
	chosen, layoutBase, err := selectViews(viewsCtx, lister, requestedViews, cfg.Views, established, d.prompt, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return 1
	}
	// The SAME gate `config set views.selected` uses. pair skipped it in the first
	// version of this feature, and that gap alone was enough: a re-pair of an
	// established single-view install flipped the layout, and the next cycle
	// reported the whole tree as locally deleted - to the server.
	if err := guardViewChange(d.store, layoutBase, chosen, cfg.SyncRoot, stderr); err != nil {
		if !errors.Is(err, errLayoutLocked) {
			fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		}
		return 1
	}
	// Re-pairing chooses views, not exclusions: an exclusion the user set survives it.
	chosen.Excluded = cfg.Views.Excluded
	chosen.ExcludedNames = cfg.Views.ExcludedNames
	cfg.Views = chosen
	// A FIRST pairing starts with the official client's defaults (exclude.go). A re-pair keeps
	// whatever the user has, so a rule they removed does not come back.
	if !prior.paired {
		home := homeView(viewsCtx, lister, cfg.Views)
		applyDefaultExclusions(&cfg.Views, home)
		describeDefaultExclusions(stdout, cfg.Views, home)
	}
	// `config set sync_root` warns about a retarget; pair did not, and under a flat
	// layout the second half of that warning is new - the sync root IS the view, so
	// whatever already sits in the new one is uploaded on the first cycle.
	if prior.syncRoot != "" && prior.syncRoot != cfg.SyncRoot {
		warnSyncRootChange("sync_root", cfg, stderr)
	}

	// Store the pin before the session: if the second write fails, a lone pin
	// leaves `login` able to re-pair, whereas a lone session cannot be used. The
	// whole config was already validated up front, before the pairing turn, so an
	// invalid blob never reaches these keychain writes.
	server := addr
	if err := d.kr.SetPin(server, sess.Pin()); err != nil {
		printKeyringErr(stderr, "store cert pin", err)
		return 1
	}
	if err := d.kr.SetSession(server, cfg.Server.User, session); err != nil {
		printKeyringErr(stderr, "store session", err)
		return 1
	}

	// Final config write (records the paired state authoritatively; for a fresh
	// device_uuid it re-writes the same blob already persisted up front). Routed
	// through a package var so a test can simulate a failure of this final write -
	// the exact window the up-front device_uuid persist closes - and assert a
	// retry reuses the same device_uuid.
	if err := putPairedConfig(d.store, cfg); err != nil {
		fmt.Fprintf(stderr, "synosync: save config: %s\n", userErr(err))
		return 1
	}

	// Re-pairing to a NEW connection identity on an already-paired install orphans
	// the OLD identity's keychain secrets (session token + certificate pin) -
	// exactly the class `config set server.*` cleans via
	// cleanupOrphanedServerSecrets. The sibling pair path had
	// no such cleanup: an operator migrating with `pair --host nasB` (no unlink)
	// left nasA's server-valid session token and pin in the keychain forever, while
	// `unlink` - keyed only on the CURRENT server.* - reported full cleanup. Run it
	// only AFTER putPairedConfig, so a failed write never erases secrets the
	// still-current blob points at.
	cleanupPairOrphanedSecrets(prior, cfg, stderr)

	// Show the pinned fingerprint so the operator can record it and verify it
	// out-of-band (and reuse it via --expect-pin on a future re-pair).
	fmt.Fprintf(stdout, "certificate pin: %s\n", sess.Pin())
	fmt.Fprintf(stdout, "paired with %s as %s\n", addr, cfg.Server.User)
	// Pairing is the hardest step - a password, often a one-time code - and it
	// used to end here, leaving the user with a pinned certificate and no idea
	// that nothing syncs until a daemon runs. Name the next two commands at the
	// one moment the user is certain to be reading.
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Next:")
	fmt.Fprintln(stdout, "  synosync service install    sync in the background (Windows: prints the command to run)")
	fmt.Fprintln(stdout, "  synosync status             check that it connected")
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "Or run `synosync daemon` in the foreground to watch it work.")
	return 0
}

// cleanupPairOrphanedSecrets erases the keychain secrets orphaned by a re-pair
// that changed the connection identity on an already-paired install.
// It is the pair-path companion of cleanupOrphanedServerSecrets: it gates
// on the PRE-overlay paired state (a fresh install stored nothing under an old
// identity) and then funnels through the same cleanupIdentitySecrets core, so the
// two identity-change paths erase an identical set (session+password by the old
// pair, pin by the old server only when host/port changed). It deliberately does
// NOT emit warnServerIdentityChange's message - that warns the new identity is
// "not paired … until you re-pair", which is false here: pair just re-paired it.
func cleanupPairOrphanedSecrets(prior priorIdentity, newCfg config.Config, stderr io.Writer) {
	if !prior.paired {
		return // fresh install - no secrets were stored under an old identity
	}
	cleanupIdentitySecrets(prior.server, prior.user, serverAddr(newCfg), newCfg.Server.User, stderr)
}

// pinMatches reports whether the operator-supplied expected pin matches the pin
// the server actually presented. The expected value may be the full
// "spki-sha256:<hex>" form printed by a recent pairing, the legacy
// "sha256:<hex>" full-DER form an operator recorded before the SPKI switch, or
// just the bare hex digest; comparison ignores surrounding whitespace and ASCII
// case. A legacy-DER expected pin (transport.IsLegacyPin) is compared against the
// leaf's DER fingerprint; everything else against its SPKI fingerprint, since the
// two hash different certificate bytes and would never match across forms.
func pinMatches(expected, spki, der string) bool {
	norm := func(s string) string {
		s = strings.ToLower(strings.TrimSpace(s))
		s = strings.TrimPrefix(s, transport.SPKIPrefix)
		s = strings.TrimPrefix(s, transport.PinPrefix)
		return s
	}
	// Dispatch on the operator-supplied prefix case-insensitively: an operator may
	// type "SHA256:" in any case, so lower-case before classifying the form.
	actual := spki
	if transport.IsLegacyPin(strings.ToLower(strings.TrimSpace(expected))) {
		actual = der
	}
	return norm(expected) == norm(actual)
}

// applyServerFlags overlays non-empty command-line overrides onto the server
// coordinates loaded from config.
func applyServerFlags(cfg *config.Config, host string, port int, user string) {
	if host != "" {
		cfg.Server.Host = host
	}
	if port != 0 {
		cfg.Server.Port = port
	}
	if user != "" {
		cfg.Server.User = user
	}
}

// pairErrorMessage renders an auth error as a concise user-facing message rather
// than a wrapped chain, keeping the typed OTP/version cases friendly.
func pairErrorMessage(err error) string {
	switch {
	case errors.Is(err, auth.ErrUserNotFound):
		return "the server does not know this account name"
	case errors.Is(err, auth.ErrInvalidPassword):
		return "the server rejected this password"
	case errors.Is(err, auth.ErrOTPInvalid):
		return "incorrect or already-used one-time password"
	case errors.Is(err, auth.ErrOTPRequired):
		return "this account requires a one-time password"
	case errors.Is(err, auth.ErrClientVersion):
		return "the server rejected this client version"
	default:
		return fmt.Sprintf("pairing failed: %v", err)
	}
}

// trustServerCertificate decides, BEFORE the password goes out, whether to go on with a server
// whose certificate this computer does not trust. The pin captured here is what every later
// connection is checked against, so first contact is the one moment an interception could be
// pinned for good. A chain to a trusted root for this host passes quietly; anything else is
// said out loud and needs a yes, the --expect-pin confirmation, or --accept-untrusted. The
// official client asks the same question at the same point.
func trustServerCertificate(p prompter, sess *transport.Session, host string, pinConfirmed, accept bool, stdout, stderr io.Writer) bool {
	certs := sess.PeerCertificates()
	err := transport.VerifyServerCertificate(certs, host)
	if err == nil {
		fmt.Fprintf(stdout, "certificate: trusted, %s\n", transport.DescribeCertificate(certs[0]))
		return true
	}
	desc := "no certificate"
	if len(certs) > 0 {
		desc = transport.DescribeCertificate(certs[0])
	}
	fmt.Fprintf(stderr, "synosync: the server's certificate is not trusted by this computer: %s\n", transport.TrustHint(err, host))
	fmt.Fprintf(stderr, "synosync:   %s\n", desc)
	fmt.Fprintf(stderr, "synosync:   key: %s\n", sess.Pin())
	switch {
	case pinConfirmed:
		fmt.Fprintln(stderr, "synosync: the key matches --expect-pin, so it was verified out of band; continuing")
		return true
	case accept:
		fmt.Fprintln(stderr, "synosync: continuing because of --accept-untrusted; this key is pinned from now on")
		return true
	}
	ok, perr := p.Confirm("Trust this certificate and pin its key? [y/N]: ")
	if perr != nil || !ok {
		fmt.Fprintln(stderr, "synosync: not paired. If this is your NAS's own certificate, re-run with --accept-untrusted,")
		fmt.Fprintln(stderr, "synosync:   or verify the key above out of band and pass it as --expect-pin.")
		return false
	}
	return true
}
