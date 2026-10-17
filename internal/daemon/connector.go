package daemon

// connector.go composes the keychain + config into a transport.Connector the pool
// can call to dial a fresh authenticated Session: it dials verifying the pinned
// certificate fingerprint (verify mode) and presents the stored session token (mode A reuse),
// mirroring the `login` command's reuse path. A vanished session (8193) surfaces
// as auth.ErrSessionVanished so the pool pauses and the daemon stops for re-login.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/transport"
)

// connectAttemptTimeout bounds one full connect attempt - TCP dial,
// encrypt_channel, TLS upgrade and the mode-A session-reuse turn. Every pool
// dial path (Start, Replace, ReplaceEvent) runs on the daemon's long-lived run
// context, which carries no deadline of its own - deliberately, per the pool's
// documented long-lived-ctx invariant - so without a per-attempt bound a server
// whose TCP stack accepts the connection but whose service never responds (a
// hung DSM, a middlebox swallowing data) would block transport.Connect or
// auth.Reuse forever, silently wedging pool bring-up at boot or the engine's
// single-threaded re-dial path mid-run. netctx.Watch makes
// the deadline effective on every I/O stage of the attempt.
//
// The value is generous against a slow DSM (a healthy attempt is a handful of
// round-trips), and no config key is added. On expiry the attempt fails with an
// ordinary transient error (context.DeadlineExceeded - not
// auth.ErrSessionVanished or transport.ErrPinMismatch), so the pool's reconnect
// backoff and the daemon's bring-up backoff keep retrying rather than pausing;
// and because the pool's reconnect loop consumes per-attempt failures
// internally, the error never reaches the engine's isTerminal classifier
// (which reads a raw DeadlineExceeded as the run context's own expiry).
// A var only so tests can shorten it.
var connectAttemptTimeout = 60 * time.Second

// connSecrets is the slice of the keychain NewConnector reads. Narrowed to an
// interface so connector tests substitute a fake without the system keychain.
type connSecrets interface {
	// Pin returns the pinned certificate fingerprint for the server (SPKI
	// public-key form, or a legacy leaf-DER pin until migrated).
	Pin(server string) (string, error)
	// Session returns the stored session token for the server/user pair.
	Session(server, user string) (string, error)
	// SetPin re-stores the pinned fingerprint, used to migrate a legacy full-DER
	// pin to the SPKI form once the same certificate has verified.
	SetPin(server, fingerprint string) error
}

// serverAddr is the "host:port" dial target and keychain server key for cfg,
// mirroring the cmd-layer helper.
func serverAddr(cfg config.Config) string {
	return net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port))
}

// NewConnector builds a transport.Connector from the config and keychain plus the
// stored session token. It reads the pinned certificate fingerprint and the session
// token up front (so a missing/blank pin or session is reported once, clearly, as
// a re-pair/re-login signal rather than silently defeating pinning at dial time),
// and returns a connect func that dials the paired server verifying the pin and
// presents the session token via mode-A reuse. The session token is returned
// alongside so the caller can use it as the engine's selfSessionID and action
// auth token without re-reading the keychain.
//
// A non-nil log receives a single info line when a stored legacy full-DER pin is
// transparently upgraded to the SPKI form after a successful reuse (a routine
// re-issue with the same keypair then no longer breaks the sync). A nil log
// disables that notice; the migration itself still runs.
func NewConnector(cfg config.Config, kr connSecrets, client auth.ClientInfo, log *slog.Logger) (transport.Connector, string, error) {
	server := serverAddr(cfg)

	pin, err := kr.Pin(server)
	if err != nil {
		if errors.Is(err, secrets.ErrNotFound) {
			return nil, "", fmt.Errorf("daemon: no pinned certificate for %s; run `synosync pair` first", server)
		}
		return nil, "", fmt.Errorf("daemon: read cert pin: %w", err)
	}
	// A present-but-empty pin must never reach transport.Connect: an empty Pin
	// there selects capture mode (trust any leaf), silently defeating pinning.
	if pin == "" {
		return nil, "", fmt.Errorf("daemon: stored certificate pin for %s is empty; re-pair with `synosync pair`", server)
	}

	session, err := kr.Session(server, cfg.Server.User)
	if err != nil {
		if errors.Is(err, secrets.ErrNotFound) {
			return nil, "", fmt.Errorf("daemon: no session for %s; run `synosync login` first", server)
		}
		return nil, "", fmt.Errorf("daemon: read session: %w", err)
	}
	if session == "" {
		return nil, "", fmt.Errorf("daemon: stored session for %s is empty; run `synosync login` first", server)
	}

	agent := client.Agent(cfg.DeviceUUID)
	// activePin is the pin currently in force for dialing. It starts as the stored
	// pin and is upgraded in place to the SPKI form after a successful legacy→SPKI
	// migration, so a long-lived daemon that has migrated also survives a later
	// same-key certificate re-issue without a restart (the keychain alone holding
	// the SPKI form would not help an already-running process). It is guarded by
	// pinMu because the pool dials its workers - and re-dials dropped connections -
	// concurrently, so reads and the one-shot migration write can race.
	var (
		pinMu     sync.RWMutex
		activePin = pin
	)
	// mig upgrades a stored legacy full-DER pin to SPKI at most once per connector
	// lifetime, so a re-dialing pool does not re-store or re-log it, and it keeps
	// the keychain write off the dial path's critical section (see pinMigration).
	mig := &pinMigration{}
	connect := func(ctx context.Context) (*transport.Session, error) {
		// Bound this attempt (dial + encrypt_channel + TLS + reuse) so an
		// accepting-but-silent server fails the attempt instead of hanging the
		// caller forever; the parent ctx stays the overall cancellation authority
		// (an earlier parent deadline/cancel still wins). See connectAttemptTimeout.
		ctx, cancel := context.WithTimeout(ctx, connectAttemptTimeout)
		defer cancel()
		pinMu.RLock()
		dialPin := activePin
		pinMu.RUnlock()
		// A ErrPinMismatch from transport.Connect (a genuine key change) flows out
		// %w-wrapped, so the daemon's caller can errors.Is it to a clear re-pair
		// signal rather than retrying a doomed dial.
		sess, err := transport.Connect(ctx, transport.ConnectConfig{
			Addr:  server,
			Agent: agent,
			DsID:  cfg.Server.DsID,
			Pin:   dialPin,
		})
		if err != nil {
			return nil, fmt.Errorf("daemon: connect %s: %w", server, err)
		}
		// Mode A: confirm the stored session token is still valid server-side. A
		// vanished session (8193) is returned as auth.ErrSessionVanished, which the
		// pool maps to its terminal paused state (the %w wrap preserves errors.Is).
		if err := auth.Reuse(ctx, sess.Conn(), session, agent, cfg.Server.DsID); err != nil {
			_ = sess.Close()
			return nil, fmt.Errorf("daemon: reuse session for %s: %w", server, err)
		}
		// The certificate has now verified under the stored pin; if that pin is a
		// legacy full-DER hash, transparently upgrade it to the SPKI form so a
		// routine re-issue with the same keypair stops breaking the sync. The
		// legacy pin keeps verifying until this succeeds; a store failure (or a
		// keychain that never answers) is logged but never fails the connect. On
		// success adopt the SPKI pin in memory too, not just in the keychain, so
		// later re-dials in this process use it.
		mig.run(ctx, kr, server, dialPin, sess.SPKIPin(), log, func(upgraded string) {
			pinMu.Lock()
			activePin = upgraded
			pinMu.Unlock()
		})
		return sess, nil
	}
	return connect, session, nil
}

// migratePinWait bounds how long a connect attempt waits for the one-shot
// legacy→SPKI keychain write to finish. The write goes through go-keyring, which
// is NOT context-aware: on a headless Linux box (Secret Service) or macOS
// (/usr/bin/security) a locked or prompting backend can block the call for an
// unbounded time, and neither connectAttemptTimeout (effective only on the
// network stages, via netctx/HandshakeContext) nor a cancel of the pool's
// reconnect context can interrupt it. Running it inline therefore broke the
// pool's documented "Close interrupts an in-flight connect" invariant and could
// wedge pool bring-up with Reconnecting() false - an invisible hang.
// The write now runs on its own goroutine and the dial path waits at
// most this long for it, treating an expiry exactly like the already-tolerated
// store failure: warn, keep the legacy pin (which still verifies), hand the
// session to the pool. A var only so tests can shorten it.
var migratePinWait = 5 * time.Second

// pinMigration runs the legacy→SPKI keychain upgrade off the dial path's
// critical section: at most one attempt is in flight at a time and at most one
// succeeds per connector lifetime, while a caller never blocks on the keychain
// for longer than migratePinWait (or until its context is cancelled).
type pinMigration struct {
	mu sync.Mutex
	// running is true while an upgrade goroutine has not returned. A goroutine
	// blocked forever inside the keychain therefore also suppresses further
	// attempts, so a wedged backend cannot accumulate one goroutine per dial.
	running bool
	// done is true once the pin has actually been re-stored. A failed attempt
	// leaves it false so a later connect retries - the write is idempotent.
	done bool
}

// run performs the upgrade of a legacy pin to the presented leaf's SPKI form,
// calling adopt with the new pin once the keychain write succeeds. It returns as
// soon as the write completes, migratePinWait elapses, or ctx is cancelled -
// whichever comes first - because the write itself cannot be interrupted. A
// pin that needs no upgrade (already SPKI, or no SPKI form presented), an
// attempt already in flight, and an already-completed migration all return
// immediately without touching the keychain.
//
// adopt may therefore be called after run has returned, from the upgrade
// goroutine; it must be safe to call concurrently with the dial path.
func (m *pinMigration) run(ctx context.Context, kr connSecrets, server, pin, spki string, log *slog.Logger, adopt func(string)) {
	if !transport.IsLegacyPin(pin) || spki == "" {
		return
	}
	m.mu.Lock()
	if m.done || m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		upgraded := migrateLegacyPin(kr, server, pin, spki, log)
		if upgraded != "" {
			adopt(upgraded)
		}
		m.mu.Lock()
		m.running = false
		m.done = upgraded != ""
		m.mu.Unlock()
	}()

	timer := time.NewTimer(migratePinWait)
	defer timer.Stop()
	select {
	case <-finished:
	case <-timer.C:
		if log != nil {
			log.Warn("cert pin SPKI upgrade did not complete in time; keeping the stored pin",
				"server", server, "wait", migratePinWait)
		}
	case <-ctx.Done():
		if log != nil {
			log.Warn("cert pin SPKI upgrade abandoned; keeping the stored pin",
				"server", server, "err", ctx.Err())
		}
	}
}

// migrateLegacyPin re-stores the presented leaf's SPKI fingerprint (spki) when the
// stored pin is in the legacy full-DER format, returning the SPKI pin now in force
// so the caller can adopt it in memory as well. It returns "" (no upgrade) when the
// pin is already SPKI, spki is empty, or the store failed - in each case the
// existing pin keeps verifying. A store failure is reported (when log is non-nil)
// but never fatal.
func migrateLegacyPin(kr connSecrets, server, pin, spki string, log *slog.Logger) string {
	if !transport.IsLegacyPin(pin) {
		return ""
	}
	if spki == "" {
		return ""
	}
	if err := kr.SetPin(server, spki); err != nil {
		if log != nil {
			log.Warn("upgrade cert pin to SPKI", "server", server, "err", err)
		}
		return ""
	}
	if log != nil {
		log.Info("certificate pin upgraded to SPKI", "server", server)
	}
	return spki
}
