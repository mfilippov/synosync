//go:build integration

// Package integration holds the end-to-end test harness for synosync (DESIGN
// §11/§12 Phase 10b). It runs the real sync stack - transport.Pool over a
// keychain-free Connector, actions.Client, store and engine - against a live
// test Synology DSM.
//
// The whole package is behind the `integration` build tag so the default
// `make test` / CI never compile or run it. Even under the tag every scenario
// self-skips (requireEnv) when the SYNOSYNC_IT_* environment is absent, so
// `go test -tags integration` is safe to run without a stand.
package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/engine"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
	"github.com/mfilippov/synosync/internal/watcher"
)

// Environment-variable names that configure the integration harness. HOST,
// PORT, USER and PASS are required to run; the rest are optional.
const (
	envHost       = "SYNOSYNC_IT_HOST"
	envPort       = "SYNOSYNC_IT_PORT"
	envUser       = "SYNOSYNC_IT_USER"
	envPass       = "SYNOSYNC_IT_PASS" //nolint:gosec // env-var name, not a hardcoded credential
	envOTP        = "SYNOSYNC_IT_OTP"
	envView       = "SYNOSYNC_IT_VIEW"
	envRemoteBase = "SYNOSYNC_IT_REMOTE_BASE"
)

// defaultRemoteBase is the scratch base path the harness owns and cleans when
// SYNOSYNC_IT_REMOTE_BASE is unset. It is a sub-path the test exclusively
// manages so it never touches unrelated server data.
const defaultRemoteBase = "/synosync-it"

// harnessConfig holds the resolved SYNOSYNC_IT_* environment a scenario needs to
// reach the test DSM. It is produced by requireEnv.
type harnessConfig struct {
	// Host is the DSM hostname or IP (SYNOSYNC_IT_HOST).
	Host string
	// Port is the sproto TCP port (SYNOSYNC_IT_PORT).
	Port int
	// User is the login account name (SYNOSYNC_IT_USER).
	User string
	// Pass is the account password (SYNOSYNC_IT_PASS); used to pair/login.
	Pass string
	// OTP is the optional one-time password (SYNOSYNC_IT_OTP) for 2FA accounts.
	OTP string
	// View optionally pins the view name the scenarios sync (SYNOSYNC_IT_VIEW);
	// empty means the harness uses the account's first writable view.
	View string
	// RemoteBase is the scratch server sub-path the harness owns and cleans
	// (SYNOSYNC_IT_REMOTE_BASE), defaulting to defaultRemoteBase.
	RemoteBase string
}

// requireEnv reads the SYNOSYNC_IT_* environment into a harnessConfig. When any
// required variable is missing (or PORT is non-numeric) it t.Skips with a clear
// message listing exactly what is absent, so the scenario is a clean skip rather
// than a failure on a host with no DSM stand.
func requireEnv(t *testing.T) harnessConfig {
	t.Helper()

	var missing []string
	get := func(name string) string {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}

	cfg := harnessConfig{
		Host: get(envHost),
		User: get(envUser),
		Pass: get(envPass),
	}
	portStr := get(envPort)
	// Optional values do not contribute to the missing list.
	cfg.OTP = strings.TrimSpace(os.Getenv(envOTP))
	cfg.View = strings.TrimSpace(os.Getenv(envView))
	cfg.RemoteBase = strings.TrimSpace(os.Getenv(envRemoteBase))
	if cfg.RemoteBase == "" {
		cfg.RemoteBase = defaultRemoteBase
	}

	if len(missing) > 0 {
		t.Skipf("integration harness disabled: set %s to run (see docs/INTEGRATION.md)", strings.Join(missing, ", "))
	}

	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		t.Skipf("integration harness disabled: %s=%q is not a valid TCP port", envPort, portStr)
	}
	cfg.Port = port

	return cfg
}

// baseConfig returns a config.Config wired to the test DSM with the harness's
// temp sync root, ready for the connect/engine helpers. It is valid
// except for any server-paired fields (DsID/DeviceUUID) filled in during connect.
func (h harnessConfig) baseConfig(syncRoot string) config.Config {
	c := config.Default()
	c.SyncRoot = syncRoot
	c.Server.Host = h.Host
	c.Server.Port = h.Port
	c.Server.User = h.User
	return c
}

// tempSyncRoot returns a fresh per-test local sync-root directory. testing's
// TempDir removes it on cleanup.
func tempSyncRoot(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

// tempPaths returns a config.Paths rooted at per-test temp directories so the
// harness's state.db and logs never collide with a developer's real install.
// testing's TempDir removes them on cleanup.
func tempPaths(t *testing.T) config.Paths {
	t.Helper()
	return config.Paths{
		StateDir: t.TempDir(),
		LogDir:   t.TempDir(),
	}
}

// scratchRemoteBase derives a unique server-side scratch path under the
// configured RemoteBase, suffixed from the test name AND the process id, so
// concurrent runs of the same test (two `make test-integration` processes against
// one DSM) and repeated runs never collide on the same remote tree - a shared name
// would let one run's pre-run clearRemote delete the other run's in-flight tree.
// The returned path always uses forward slashes (the server path separator).
func scratchRemoteBase(t *testing.T, h harnessConfig) string {
	t.Helper()
	suffix := sanitizeName(t.Name()) + "_p" + strconv.Itoa(os.Getpid())
	base := strings.TrimRight(h.RemoteBase, "/")
	if base == "" {
		base = defaultRemoteBase
	}
	return path(base, suffix)
}

// path joins server-side path segments with a single forward slash, the server
// path separator (filepath.Join would use the host separator on Windows).
func path(segs ...string) string {
	cleaned := make([]string, 0, len(segs))
	for _, s := range segs {
		s = strings.Trim(s, "/")
		if s != "" {
			cleaned = append(cleaned, s)
		}
	}
	return "/" + strings.Join(cleaned, "/")
}

// sanitizeName reduces a test name to a filesystem/server-safe token: slashes
// and other separators become underscores so a subtest's "/"-joined name is a
// single path segment.
func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" {
		s = "scratch"
	}
	return s
}

// localJoin joins a view-relative slash path onto a local sync-root directory
// using the host path separator, rejecting an escaping path.
func localJoin(root, rel string) (string, error) {
	rel = strings.TrimLeft(rel, "/")
	cleanRoot := filepath.Clean(root)
	joined := filepath.Join(cleanRoot, filepath.FromSlash(rel))
	// Compare against root with a trailing separator so a sibling sharing a name
	// prefix (e.g. /tmp/abc vs /tmp/ab) is not mistaken for a containment.
	if joined != cleanRoot && !strings.HasPrefix(joined, cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("integration: path %q escapes sync root", rel)
	}
	return joined, nil
}

// netTimeout bounds a single harness network exchange (pair, connect, a server
// action turn). It is generous so a slow stand or a large transfer does not flake.
const netTimeout = 60 * time.Second

// clientInfo is the static identity the harness announces to the test DSM,
// mirroring cmd/synosync's clientInfo so the server treats the harness like a
// real desktop client.
func (h harnessConfig) clientInfo() auth.ClientInfo {
	// Must mirror cmd/synosync's clientInfo: a live DSM validates the type and the
	// version string on every action turn, not just at auth.
	return auth.ClientInfo{
		Platform: platformName(),
		Type:     "drive",
		Major:    4, Minor: 0, Mini: 2, Build: 17889,
	}
}

// platformName maps the Go OS name onto the platform label the server expects,
// mirroring cmd/synosync's helper of the same name.
func platformName() string {
	switch runtime.GOOS {
	case "darwin":
		return "mac"
	case "windows":
		return "windows"
	default:
		return "linux"
	}
}

// actionCtx returns a context bounded by netTimeout plus its cancel, for a single
// harness network turn.
func actionCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), netTimeout)
}

// pairing holds one paired device's reusable session: a transport.Connector that
// re-dials fully-authenticated sessions (mode-A auth.Reuse over the captured leaf
// pin) plus the issued session token and the learned server identity. The pin and
// token stay in this process (no system keychain), mirroring the `pair` command
// followed by daemon.NewConnector.
type pairing struct {
	// connector dials the paired server verifying the pin and presents the session
	// via auth.Reuse - exactly what transport.Pool calls per worker.
	connector transport.Connector
	// session is the issued session token: the engine's selfSessionID and the
	// action auth token. Two distinct pairings carry two distinct tokens, which the
	// server stamps as distinct committer_session_ids.
	session string
	// dsID is the server identity learned from TEST_CONN.
	dsID string
	// deviceUUID is this device's stable identifier (one per pairing).
	deviceUUID string
}

// pairDevice performs one mode-C pairing with the test DSM and returns a reusable
// pairing. It probes for the ds_id (TEST_CONN), brings up a capture-mode connection
// to learn the leaf pin, and authenticates with the env password and the supplied
// otp (auth.Pair). Each call generates a fresh device_uuid, so two calls produce two
// independent server sessions.
func pairDevice(ctx context.Context, h harnessConfig, otp string) (pairing, error) {
	addr := net.JoinHostPort(h.Host, strconv.Itoa(h.Port))

	reply, err := transport.TestConn(ctx, addr)
	if err != nil {
		return pairing{}, fmt.Errorf("probe %s: %w", addr, err)
	}
	// The legacy TEST_CONN frame reports success as 1, not 0 - Succeeded owns that.
	if !reply.Succeeded() || reply.DsID == "" {
		return pairing{}, fmt.Errorf("probe %s: server returned no ds_id (result %d)", addr, reply.Result)
	}
	dsID := reply.DsID

	deviceUUID, err := auth.NewDeviceUUID()
	if err != nil {
		return pairing{}, fmt.Errorf("generate device_uuid: %w", err)
	}

	ci := h.clientInfo()
	agent := ci.Agent(deviceUUID)

	// Empty Pin → capture mode: trust this leaf and record its fingerprint.
	sess, err := transport.Connect(ctx, transport.ConnectConfig{
		Addr:  addr,
		Agent: agent,
		DsID:  dsID,
	})
	if err != nil {
		return pairing{}, fmt.Errorf("connect %s: %w", addr, err)
	}
	pin := sess.Pin()

	creds := auth.Credentials{User: h.User, Password: h.Pass, OTP: otp}
	session, err := auth.Pair(ctx, sess.Conn(), ci, deviceUUID, dsID, creds)
	_ = sess.Close()
	if err != nil {
		return pairing{}, fmt.Errorf("pair with %s: %w", addr, err)
	}

	connector := func(ctx context.Context) (*transport.Session, error) {
		s, err := transport.Connect(ctx, transport.ConnectConfig{
			Addr:  addr,
			Agent: agent,
			DsID:  dsID,
			Pin:   pin,
		})
		if err != nil {
			return nil, fmt.Errorf("integration: connect %s: %w", addr, err)
		}
		if err := auth.Reuse(ctx, s.Conn(), session, agent, dsID); err != nil {
			_ = s.Close()
			return nil, fmt.Errorf("integration: reuse session for %s: %w", addr, err)
		}
		return s, nil
	}
	return pairing{connector: connector, session: session, dsID: dsID, deviceUUID: deviceUUID}, nil
}

// Package-level pairing cache. The harness pairs exactly two devices ONCE for the
// whole package run - not once per scenario - for two reasons. First, the engine
// recognises an event whose committer_session_id equals its own session as a
// self-echo and only advances the baseline without touching disk (DESIGN §6); a
// server→local scenario therefore needs the server-mutation client to commit under
// a DIFFERENT session than the engine, so we pair a dedicated "mutator" device whose
// distinct session id is never mistaken for the engine's own. Second, pairing is
// expensive and consumes a one-time password (SYNOSYNC_IT_OTP): pairing per scenario
// would burn the single OTP on the first scenario, fail the rest as "OTP already
// used", and leave a paired device per scenario behind. Caching pins the device
// count at two for the run.
var (
	pairMu      sync.Mutex
	pairDone    bool
	pairErr     error
	pairEngine  pairing
	pairMutator pairing

	// pairPeer is the THIRD device: the second full sync client the two-client
	// scenarios run (twoclient_test.go). It is paired lazily - only a scenario that
	// spawns a peer pays for it - and its own session id is what makes the peer's
	// commits ordinary remote events for the first engine rather than self-echoes.
	pairPeerDone bool
	pairPeerErr  error
	pairPeer     pairing
)

// ensurePaired returns the cached engine and mutator pairings, performing the
// two pairings on first use under the package lock. A pairing failure is cached so
// every subsequent scenario fails the same way instead of re-attempting (and
// re-consuming credentials). The mutator is paired with no OTP, since the single
// SYNOSYNC_IT_OTP is consumed by the engine pairing; a 2FA-enforced test account
// that rejects the second device must therefore be run without enforced 2FA (or
// with an app password) - see docs/INTEGRATION.md.
func ensurePaired(t *testing.T, h harnessConfig) (engine, mutator pairing) {
	t.Helper()
	pairMu.Lock()
	defer pairMu.Unlock()
	if !pairDone {
		pairDone = true
		ctx, cancel := context.WithTimeout(context.Background(), 2*netTimeout)
		defer cancel()
		eng, err := pairDevice(ctx, h, h.OTP)
		if err != nil {
			pairErr = fmt.Errorf("pair engine device: %w", err)
		} else {
			// Publish the engine pairing BEFORE attempting the mutator. pairDevice
			// returns only after auth.Pair succeeded, so at this point the device IS
			// registered on the server; holding it in a local until BOTH pairings
			// succeed meant a mutator failure discarded the one registration the run
			// had already created, and unlinkPairedDevices had nothing to release -
			// leaking a dead client into the stand's connection list on every failed
			// setup.
			pairEngine = eng
			if mut, merr := pairDevice(ctx, h, ""); merr != nil {
				pairErr = fmt.Errorf("pair mutator device (a distinct session is required so server→local changes are not self-echoed; a 2FA account may reject the second device): %w", merr)
			} else {
				pairMutator = mut
			}
		}
	}
	if pairErr != nil {
		t.Fatalf("integration: %v", pairErr)
	}
	return pairEngine, pairMutator
}

// ensurePeerPaired returns the cached third pairing - the second sync CLIENT's device
// - pairing it on first use under the package lock. It is separate from ensurePaired
// so a run that never spawns a peer registers only the usual two devices. Like the
// engine's, a failure is cached so every later scenario fails identically instead of
// re-attempting with the same credentials.
func ensurePeerPaired(t *testing.T, h harnessConfig) pairing {
	t.Helper()
	pairMu.Lock()
	defer pairMu.Unlock()
	if !pairPeerDone {
		pairPeerDone = true
		ctx, cancel := context.WithTimeout(context.Background(), netTimeout)
		defer cancel()
		// No OTP: the single SYNOSYNC_IT_OTP is consumed by the engine pairing, exactly
		// as for the mutator device (see ensurePaired).
		p, err := pairDevice(ctx, h, "")
		if err != nil {
			pairPeerErr = fmt.Errorf("pair peer device (the second sync client needs its own session so its commits are not self-echoed): %w", err)
		} else {
			pairPeer = p
		}
	}
	if pairPeerErr != nil {
		t.Fatalf("integration: %v", pairPeerErr)
	}
	return pairPeer
}

// unlinkPairedDevices releases the two device registrations the run created, once the
// whole package has finished (TestMain). Without it a stand accumulates a PAIR of
// registered devices per `make test-integration` - the harness pairs mode-C rather than
// reusing a stored identity, so nothing else ever drops them, and a long-lived test DSM
// ends up with hundreds of dead clients in its Drive connection list.
//
// It is best-effort by design: the tests have already finished, so a failure here must not
// fail the run. Each device gets its own dialled session (the scenario clients are closed
// by their own t.Cleanup by now) and any error is reported on stderr, where it is visible
// without being fatal.
//
// A PARTIALLY failed setup still has registrations to release - pairing is per device, so
// an engine that paired before the mutator failed is registered exactly like one from a
// clean run. The discriminator is therefore the pairing's own session, never the aggregate
// pairErr/pairPeerErr: a cached failure gates the TESTS, not the cleanup.
func unlinkPairedDevices() {
	pairMu.Lock()
	defer pairMu.Unlock()
	for _, p := range []pairing{pairEngine, pairMutator, pairPeer} {
		if p.session == "" {
			continue
		}
		if err := unlinkPairing(p); err != nil {
			fmt.Fprintf(os.Stderr, "integration: unlink device %s: %v\n", p.deviceUUID, err)
		}
	}
}

// unlinkPairing dials one authenticated session for the pairing and asks the server to
// drop this connection's registration, mirroring what `synosync unlink` does.
func unlinkPairing(p pairing) error {
	ctx, cancel := context.WithTimeout(context.Background(), netTimeout)
	defer cancel()
	sess, err := p.connector(ctx)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = sess.Close() }()
	h := harnessConfig{User: os.Getenv(envUser)}
	ci := h.clientInfo()
	c := actions.NewClientWithIdentity(sess, p.session, actions.Identity{
		Agent:         ci.Agent(p.deviceUUID),
		ClientVersion: ci.ClientVersion(),
		ClientType:    ci.Type,
		DsID:          p.dsID,
	}, nil)
	return c.UnlinkConnection(ctx)
}

// discardLogger is a slog logger that drops every record. The engine runs on its
// own goroutine and may log after a test returns, so routing to t.Log would panic;
// discarding keeps the harness quiet and safe.
// discardLogger silences the engine by default so a passing run is quiet. Set
// SYNOSYNC_IT_LOG to a level name (debug/info/warn/error) to route the engine's
// diagnostics to stderr instead - the only way to see WHY a scenario's
// waitForSync timed out, since the engine swallows per-op failures as retries.
func discardLogger() *slog.Logger {
	lvl, ok := os.LookupEnv("SYNOSYNC_IT_LOG")
	if !ok {
		return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(lvl)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

// startEngine brings up the real sync stack - transport.Pool over the connector,
// engine over a PoolTransport - and runs engine.Run on a goroutine, mirroring
// daemon.defaultRunSync. The pool is started synchronously (so a connect failure
// fails the test immediately) and both the run context and the pool are torn down
// in t.Cleanup. The returned engine is used to drive TriggerSync from waitForSync.
//
// label tags every log record this engine emits (`client=A`). A two-client scenario
// runs two engines in one process writing to one stderr, and without the tag the
// interleaved diagnostics are unreadable - which side dropped an op is the whole
// question in those scenarios.
func startEngine(t *testing.T, h harnessConfig, st *store.Store, conf config.Config, connector transport.Connector, selfSessionID, deviceUUID, dsID, label string) (*engine.Engine, func()) {
	t.Helper()

	pool := transport.NewPool(connector, conf.Limits.Workers)
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Start(ctx); err != nil {
		cancel()
		_ = pool.Close()
		t.Fatalf("integration: start connection pool: %v", err)
	}

	// The identity block is validated on EVERY action turn by a live DSM, so the
	// engine under test must carry it exactly as the daemon does.
	ci := h.clientInfo()
	log := discardLogger()
	if label != "" {
		log = log.With("client", label)
	}
	tr := engine.NewPoolTransportWithIdentity(pool, selfSessionID, actions.Identity{
		Agent:         ci.Agent(deviceUUID),
		ClientVersion: ci.ClientVersion(),
		ClientType:    ci.Type,
		DsID:          dsID,
	}, log)
	eng := engine.New(st, tr, conf.SyncRoot, selfSessionID, log)
	eng.SetLimits(conf.Limits)
	// The filesystem watcher, as the daemon installs it. Without one a local change waits for the
	// cycle, which an idle long_poll holds for up to 300 s; the scenarios used to get prompt local
	// scans only because every poll was answered at once (docs/INVARIANTS.md#long_poll-must-be-able-to-hold).
	eng.SetWatcher(func(ctx context.Context, root string) (<-chan struct{}, error) {
		return watcher.New(root, log, watcher.WithDebounce(time.Duration(conf.Limits.WatcherDebounce)*time.Millisecond)).Watch(ctx)
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = eng.Run(ctx)
	}()
	// One idempotent shutdown, used both by t.Cleanup and by a scenario that stops the
	// client deliberately (scenario.stop, the "the daemon was killed" state, which is
	// NOT the same as Pause: the queue survives in the store, everything in memory does
	// not). Calling it twice is safe - cancel is idempotent, the done channel is already
	// closed, and pool.Close guards its own second call.
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
			_ = pool.Close()
		})
	}
	t.Cleanup(stop)
	return eng, stop
}

// waitForSync nudges the engine (TriggerSync) and polls predicate until it holds
// or timeout elapses, failing the test on timeout. predicate inspects whatever the
// scenario cares about - a local file's contents, a server node, a store baseline.
func waitForSync(t *testing.T, eng *engine.Engine, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		eng.TriggerSync()
		if predicate() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("integration: condition not met within %s", timeout)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// newRemoteClient dials a dedicated authenticated session (separate from the
// engine's pool) and wraps it in an actions.Client for the server-mutation
// helpers. The session is closed in t.Cleanup, and the closer is returned as well so a
// caller that REPLACES its client (redialClient) can release the outgoing session at once
// rather than leaving it live until the test ends - a retry loop that re-dials on every
// rejected turn would otherwise hold one open DSM session per iteration.
func newRemoteClient(t *testing.T, h harnessConfig, connector transport.Connector, session, deviceUUID, dsID string) (*actions.Client, func()) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	sess, err := connector(ctx)
	if err != nil {
		t.Fatalf("integration: dial remote client: %v", err)
	}
	var once sync.Once
	closeSession := func() { once.Do(func() { _ = sess.Close() }) }
	t.Cleanup(closeSession)
	ci := h.clientInfo()
	return actions.NewClientWithIdentity(sess, session, actions.Identity{
		Agent:         ci.Agent(deviceUUID),
		ClientVersion: ci.ClientVersion(),
		ClientType:    ci.Type,
		DsID:          dsID,
	}, nil), closeSession
}

// redialClient replaces the scenario's server-mutation client with a freshly dialled
// one on the same mutator pairing. It is needed after any turn that leaves the client
// broken by contract - a rejected upload in particular: the body frame is written
// concurrently with the reply read, so a server that rejects on the metadata (24579)
// leaves the connection desynced and actions.Client marks itself ErrConnBroken. The
// engine survives that by replacing the worker's session; a scenario driving the raw
// client has to do the same or every later turn fails with ErrConnBroken.
func (sc *scenario) redialClient(t *testing.T) {
	t.Helper()
	_, mut := ensurePaired(t, sc.h)
	c, closeSession := newRemoteClient(t, sc.h, mut.connector, mut.session, mut.deviceUUID, mut.dsID)
	if sc.closeClient != nil {
		// Release the session being replaced NOW. Its t.Cleanup would only fire at the end of
		// the test, so a re-dial inside a retry loop otherwise accumulates one live session per
		// attempt against the NAS.
		sc.closeClient()
	}
	sc.client, sc.closeClient = c, closeSession
}

// pickView resolves the view the scenarios sync against: the one named by
// SYNOSYNC_IT_VIEW when set, otherwise the first view the account holds writable
// (a "w" privilege bit; an empty/unknown privilege is treated as writable, matching
// the engine).
func pickView(t *testing.T, c *actions.Client, h harnessConfig) actions.View {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	views, err := c.QueryUserInfo(ctx)
	if err != nil {
		t.Fatalf("integration: query_user_info: %v", err)
	}
	if len(views) == 0 {
		t.Fatalf("integration: account has no views")
	}
	if h.View != "" {
		for _, v := range views {
			if v.Name == h.View {
				return v
			}
		}
		t.Fatalf("integration: view %q not found among %d views", h.View, len(views))
	}
	for _, v := range views {
		if v.Privilege == "" || strings.Contains(v.Privilege, "w") {
			return v
		}
	}
	t.Fatalf("integration: no writable view found")
	return actions.View{}
}

// serverMkdir creates a directory named name under parentID and returns its node.
func serverMkdir(t *testing.T, c *actions.Client, rootNodeID int64, parentID, name string) actions.Node {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	n, err := c.Mkdir(ctx, parentID, name, time.Now().Unix(), rootNodeID)
	if err != nil {
		t.Fatalf("integration: mkdir %s under %s: %v", name, parentID, err)
	}
	return n
}

// serverPut uploads content as a file named name under parentID and returns its
// node (small single-frame upload).
func serverPut(t *testing.T, c *actions.Client, rootNodeID int64, parentID, name string, content []byte) actions.Node {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	n, err := c.Upload(ctx, parentID, name, time.Now().Unix(), rootNodeID, content)
	if err != nil {
		t.Fatalf("integration: upload %s under %s: %v", name, parentID, err)
	}
	return n
}

// serverRename moves a view-relative path within a view.
func serverRename(t *testing.T, c *actions.Client, viewID, rootNodeID int64, fromPath, toPath string) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	if err := c.Rename(ctx, viewID, rootNodeID, fromPath, toPath); err != nil {
		t.Fatalf("integration: rename %s->%s in view %d: %v", fromPath, toPath, viewID, err)
	}
}

// serverRemove deletes a view-relative path within a view.
func serverRemove(t *testing.T, c *actions.Client, viewID int64, p string) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	if err := c.Remove(ctx, viewID, p); err != nil {
		t.Fatalf("integration: remove %s in view %d: %v", p, viewID, err)
	}
}

// serverRead downloads a node by file id and returns its bytes.
func serverRead(t *testing.T, c *actions.Client, rootNodeID int64, fileID string) []byte {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	var buf bytes.Buffer
	if _, err := c.Download(ctx, fileID, rootNodeID, 0, &buf); err != nil {
		t.Fatalf("integration: download %s: %v", fileID, err)
	}
	return buf.Bytes()
}

// serverQuery returns the node at a view-relative path within a view, and whether
// it exists (an ErrPathError is reported as absent, any other error fails the test).
func serverQuery(t *testing.T, c *actions.Client, viewID int64, p string) (actions.Node, bool) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	n, err := c.QueryNode(ctx, viewID, p)
	if err != nil {
		if errors.Is(err, actions.ErrPathError) {
			return actions.Node{}, false
		}
		t.Fatalf("integration: query_node %s in view %d: %v", p, viewID, err)
	}
	return n, true
}

// removeTreeRemote recursively deletes the view-relative path p within the view,
// children before parents. The server's `remove` is RECURSIVE
// (TestIntegration_RemoveDirectorySemantics), so a one-shot Remove(base) would in fact
// take the whole scratch tree - the child-first walk is retained deliberately: teardown
// then does not REST on the semantics the suite exists to measure, and it stays correct
// in both worlds if a DSM release ever changes them. It is also what makes a partial
// failure name the child it stuck on rather than the root. A path that is already absent
// (ErrPathError from the existence probe) is success. The returned error is the first
// failure encountered.
func removeTreeRemote(ctx context.Context, c *actions.Client, viewID int64, p string) error {
	n, err := c.QueryNode(ctx, viewID, p)
	if err != nil {
		if errors.Is(err, actions.ErrPathError) {
			return nil // already gone
		}
		return fmt.Errorf("query %q: %w", p, err)
	}
	if n.IsDir {
		children, listed, lerr := c.List(ctx, viewID, p)
		if lerr != nil {
			return fmt.Errorf("list %q: %w", p, lerr)
		}
		if !listed {
			// An errno-only reply enumerated nothing, so an empty child slice is not proof
			// the directory is empty - recursing no further would leave the scratch tree
			// populated and make the Remove below fail with a confusing error instead.
			return fmt.Errorf("list %q: reply carried no listing; cannot enumerate children", p)
		}
		for _, ch := range children {
			if ch.Name == "" {
				continue
			}
			if rerr := removeTreeRemote(ctx, c, viewID, strings.TrimRight(p, "/")+"/"+ch.Name); rerr != nil {
				return rerr
			}
		}
	}
	if rerr := c.Remove(ctx, viewID, p); rerr != nil {
		return fmt.Errorf("remove %q: %w", p, rerr)
	}
	return nil
}

// teardownRemote registers a best-effort cleanup that recursively removes the
// scratch base tree within the view, so a repeated run starts clean. Failures are
// ignored here (the test has already run; the path may already be absent).
func teardownRemote(t *testing.T, c *actions.Client, viewID int64, base string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := actionCtx()
		defer cancel()
		_ = removeTreeRemote(ctx, c, viewID, base)
	})
}

// clearRemote recursively removes the scratch base tree now (before a run starts)
// so stale content from a crashed prior run never interferes. Unlike teardown it
// fails loudly: a scratch base that cannot be cleared would silently contaminate the
// scenario's assertions, so the test is failed rather than run on dirty state.
func clearRemote(t *testing.T, c *actions.Client, viewID int64, base string) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()
	if err := removeTreeRemote(ctx, c, viewID, base); err != nil {
		t.Fatalf("integration: clear stale scratch tree %q: %v", base, err)
	}
}

// scenario is the assembled, running integration fixture a scenario test drives:
// the live engine over a real pool + store, a dedicated server-mutation client, the
// resolved writable view, and the scratch paths (local + server) the test owns and
// cleans. It is produced by setupScenario.
type scenario struct {
	h         harnessConfig
	conf      config.Config
	st        *store.Store
	eng       *engine.Engine
	client    *actions.Client
	connector transport.Connector
	session   string
	// deviceUUID and dsID identify the ENGINE pairing; both ride in the identity
	// block every action turn must carry.
	deviceUUID string
	dsID       string
	view       actions.View
	// viewID is view.ViewID as the store's unsigned key type.
	viewID uint64
	// base is the view-relative scratch base (no leading slash), e.g.
	// "synosync-it/TestName"; every scenario path lives under it.
	base string
	// label tags this client's engine logs ("A" for the scenario itself, "B" for a
	// spawned peer), so a two-client run's interleaved diagnostics are attributable.
	label string
	// stopEngine shuts this client's engine and pool down (idempotent). It is what a
	// scenario calls to model the daemon being STOPPED rather than paused; scenario.start
	// then brings a fresh engine up on the same store, exactly as a restart does.
	stopEngine func()
	// closeClient releases the session behind `client` (idempotent; also registered as a
	// t.Cleanup). redialClient calls it on the OUTGOING client so a retry loop that re-dials
	// on every rejected turn does not accumulate one live DSM session per attempt.
	closeClient func()
}

// stop shuts the client's engine down, modelling a killed/stopped daemon: the durable
// queue and baselines survive in state.db, every in-memory streak, cache and pending
// wake does not. It is idempotent and safe to follow with start.
func (sc *scenario) stop() {
	if sc.stopEngine != nil {
		sc.stopEngine()
	}
	sc.eng = nil
}

// setupScenario brings up the full harness for one scenario test and registers all
// teardown via t.Cleanup: requireEnv (or skip), a temp sync root + temp state.db,
// mode-C pairing, a dedicated mutation client, the resolved writable view, the
// store seeded with the view record (so the engine resolves the on-disk view
// directory to the view name from its very first scan, matching where the scenario
// writes files), a freshly-cleared per-test scratch base, and the running engine.
func setupScenario(t *testing.T) *scenario {
	t.Helper()
	sc := setupScenarioNoEngine(t)
	sc.skipColdStart(t)
	sc.start(t)
	return sc
}

// skipColdStart marks the scenario's view as already cold-started, at the event log's
// CURRENT tail, before the engine is brought up. Only the scenarios that do not TEST cold
// start use it (setupScenario); TestIntegration_KeepBothColdStart goes through
// setupScenarioNoEngine and keeps paying the real thing.
//
// It exists because a stand ACCUMULATES: the log keeps one row per live node plus a
// tombstone for every node ever deleted, and cold start replays it from sync_id 0. On a
// stand where a few thousand events had piled up, LocalToServer/ServerToLocal spent their
// whole 90 s budget replaying history before reaching their own first step and failed -
// the residual DESIGN §13 recorded on 31.07.2026, and the 10k-child listing capture
// (SYNOSYNC_IT_BIG_LIST) reproduces it in one run. Their subject is the incremental path,
// so paying for the catalog replay bought nothing but flakiness proportional to how much
// the stand had been used.
//
// The tail is taken from pull_event's next_sync_id rather than get_max_sync_id, which lags
// the log (measured 31.07.2026); a lagging cursor would only replay a few recent events,
// but the scenarios below assert on THEIR OWN changes, all committed after this point.
func (sc *scenario) skipColdStart(t *testing.T) {
	t.Helper()
	ctx, cancel := actionCtx()
	defer cancel()

	tail, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id for view %d: %v", sc.view.ViewID, err)
	}
	if _, next, err := sc.client.PullEvent(ctx, sc.view.ViewID, sc.view.RootNodeID, tail); err != nil {
		t.Fatalf("integration: pull_event to resolve the log tail: %v", err)
	} else if next > tail {
		tail = next
	}

	v, err := sc.st.GetView(sc.viewID)
	if err != nil {
		t.Fatalf("integration: read the seeded view %d: %v", sc.viewID, err)
	}
	v.InitialSyncDone = true
	v.LastSeenSyncID = tail
	if err := sc.st.PutView(sc.viewID, v); err != nil {
		t.Fatalf("integration: seed view %d as cold-started at %d: %v", sc.viewID, tail, err)
	}
	t.Logf("cold start skipped: view %d seeded as synced at sync_id %d", sc.viewID, tail)
}

// setupScenarioNoEngine performs the full scenario bring-up EXCEPT starting the
// engine, returning a scenario whose eng is nil. A scenario that must place
// server- and/or local-side state before the very first sync cycle (e.g. the
// cold-start keep-both divergence test, which needs a divergent file present on
// both sides before cold start reconciles them) calls this, seeds its pre-state,
// then calls scenario.start. setupScenario is the engine-already-running wrapper
// the inbound/outbound happy-path scenarios use.
func setupScenarioNoEngine(t *testing.T) *scenario {
	t.Helper()
	h := requireEnv(t)

	syncRoot := tempSyncRoot(t)
	paths := tempPaths(t)
	conf := h.baseConfig(syncRoot)

	engPair, mutPair := ensurePaired(t, h)
	conf.DeviceUUID = engPair.deviceUUID
	conf.Server.DsID = engPair.dsID

	st, err := store.Open(paths.StateDB())
	if err != nil {
		t.Fatalf("integration: open store %s: %v", paths.StateDB(), err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.PutConfig(conf); err != nil {
		t.Fatalf("integration: put config: %v", err)
	}

	// The server-mutation client commits under the mutator session, distinct from
	// the engine's selfSessionID, so server→local changes are seen as remote events
	// (not self-echoes) and are actually applied to disk.
	client, closeClient := newRemoteClient(t, h, mutPair.connector, mutPair.session, mutPair.deviceUUID, mutPair.dsID)
	view := pickView(t, client, h)
	viewID := uint64(view.ViewID) // #nosec G115 -- a server view_id is a non-negative identifier

	// Seed the view record so the engine's on-disk view directory is its name from
	// the first scan; refreshViews later merges its enumeration over this record
	// (preserving InitialSyncDone / LastSeenSyncID), so cold start still runs.
	if err := st.PutView(viewID, store.View{
		NodeID:     view.NodeID,
		RootNodeID: view.RootNodeID,
		Name:       view.Name,
		Privilege:  view.Privilege,
	}); err != nil {
		t.Fatalf("integration: seed view %d: %v", viewID, err)
	}

	base := strings.TrimLeft(scratchRemoteBase(t, h), "/")
	clearRemote(t, client, view.ViewID, base)
	teardownRemote(t, client, view.ViewID, base)

	return &scenario{
		h: h, conf: conf, st: st, eng: nil, client: client, closeClient: closeClient,
		connector: engPair.connector, session: engPair.session,
		deviceUUID: engPair.deviceUUID, dsID: engPair.dsID,
		view: view, viewID: viewID, base: base,
		label: "A",
	}
}

// start brings up the engine for a scenario produced by setupScenarioNoEngine,
// recording it on sc.eng (with run-context/pool teardown registered via
// t.Cleanup). It is a no-op-safe single transition: a scenario is started once.
func (sc *scenario) start(t *testing.T) {
	t.Helper()
	sc.eng, sc.stopEngine = startEngine(t, sc.h, sc.st, sc.conf, sc.connector, sc.session, sc.deviceUUID, sc.dsID, sc.label)
}

// viewRel joins scratch-base-relative slash segments onto the scenario's scratch
// base, returning a view-relative path with no leading slash - the key form the
// store and the view-scoped server actions address.
func (sc *scenario) viewRel(segs ...string) string {
	all := append([]string{sc.base}, segs...)
	return strings.TrimLeft(path(all...), "/")
}

// viewRootID returns the view's root node id to parent top-level server mkdirs
// under. The actions.View contract leaves the root NodeID optional (a DSM may omit
// it from query_user_info), mirrored by production refreshViews resolving it via
// QueryNode(viewID, ""); the harness applies the same fallback so server-side
// staging never parents under an empty "id:/..." on such a DSM.
func (sc *scenario) viewRootID(t *testing.T) string {
	t.Helper()
	if sc.view.NodeID != "" {
		return sc.view.NodeID
	}
	// The view root is addressed as "/", not the empty path: a live DSM rejects the
	// empty one with 28672 (see DESIGN §2).
	n, ok := serverQuery(t, sc.client, sc.view.ViewID, "/")
	if !ok || n.FileID == "" {
		t.Fatalf("integration: view %q has no resolvable root node id", sc.view.Name)
	}
	return n.FileID
}

// localAbs resolves a scratch-base-relative slash path to its absolute on-disk
// location under syncRoot/<viewName>/<base>/..., the location the engine's scanner
// walks for this view.
func (sc *scenario) localAbs(t *testing.T, rel string) string {
	t.Helper()
	abs, err := localJoin(sc.conf.SyncRoot, path(sc.view.Name, sc.viewRel(rel)))
	if err != nil {
		t.Fatalf("integration: resolve local path %q: %v", rel, err)
	}
	return abs
}

// writeLocal writes content to a scratch-base-relative file on disk, creating its
// parent directories. It is how a scenario produces a local→server change.
func (sc *scenario) writeLocal(t *testing.T, rel string, content []byte) {
	t.Helper()
	abs := sc.localAbs(t, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("integration: mkdir parents for %q: %v", rel, err)
	}
	if err := os.WriteFile(abs, content, 0o644); err != nil { // #nosec G306 -- a synced user file uses ordinary 0644 perms
		t.Fatalf("integration: write local %q: %v", rel, err)
	}
}

// mkdirLocal creates a scratch-base-relative directory (and parents) on disk.
func (sc *scenario) mkdirLocal(t *testing.T, rel string) {
	t.Helper()
	if err := os.MkdirAll(sc.localAbs(t, rel), 0o755); err != nil {
		t.Fatalf("integration: mkdir local %q: %v", rel, err)
	}
}

// renameLocal moves a scratch-base-relative path on disk, creating the destination
// parent first.
func (sc *scenario) renameLocal(t *testing.T, from, to string) {
	t.Helper()
	dst := sc.localAbs(t, to)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("integration: mkdir parents for %q: %v", to, err)
	}
	if err := os.Rename(sc.localAbs(t, from), dst); err != nil {
		t.Fatalf("integration: rename local %q->%q: %v", from, to, err)
	}
}

// removeLocal deletes a scratch-base-relative path tree on disk.
func (sc *scenario) removeLocal(t *testing.T, rel string) {
	t.Helper()
	if err := os.RemoveAll(sc.localAbs(t, rel)); err != nil {
		t.Fatalf("integration: remove local %q: %v", rel, err)
	}
}

// baselineState returns the store baseline state for a scratch-base-relative path
// and whether a baseline exists.
func (sc *scenario) baselineState(t *testing.T, rel string) (store.BaselineState, bool) {
	t.Helper()
	b, err := sc.st.GetBaseline(sc.viewID, sc.viewRel(rel))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", false
		}
		t.Fatalf("integration: get baseline %q: %v", rel, err)
	}
	return b.State, true
}

// queryRel queries the server node at a scratch-base-relative path and whether it
// exists.
func (sc *scenario) queryRel(t *testing.T, rel string) (actions.Node, bool) {
	t.Helper()
	return serverQuery(t, sc.client, sc.view.ViewID, sc.viewRel(rel))
}
