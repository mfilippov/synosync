package daemon

// Tests for the legacy→SPKI certificate-pin migration performed after a
// successful mode-A reuse. The keychain write is not context-aware, so the
// contract is: it never holds the pool's dial path for longer
// than migratePinWait (and a cancelled context ends the wait at once), while a
// healthy keychain still migrates the pin exactly once per connector lifetime.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/transport"
)

// migSecrets is a connSecrets stand-in whose SetPin can be made to block
// indefinitely (a locked or prompting keychain backend) and whose recorded
// writes are safe to read while a migration goroutine is still running.
type migSecrets struct {
	pin     string
	session string
	setErr  error
	// block, when non-nil, gates every SetPin call until it is closed.
	block chan struct{}

	mu      sync.Mutex
	setPins []string
	// attempts counts every SetPin CALL, successful or not. The retry contract is about
	// attempts, and setPins records only successes - so a regression that stopped retrying
	// after a failed write would be invisible to it.
	attempts int
}

func (m *migSecrets) Pin(string) (string, error)             { return m.pin, nil }
func (m *migSecrets) Session(string, string) (string, error) { return m.session, nil }

func (m *migSecrets) SetPin(_, fingerprint string) error {
	m.mu.Lock()
	m.attempts++
	m.mu.Unlock()
	if m.block != nil {
		<-m.block
	}
	if m.setErr != nil {
		return m.setErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setPins = append(m.setPins, fingerprint)
	return nil
}

// stored returns a copy of the fingerprints written so far.
func (m *migSecrets) stored() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.setPins...)
}

// attemptCount returns how many times SetPin has been entered.
func (m *migSecrets) attemptCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempts
}

// syncBuffer is a concurrency-safe log sink: the migration goroutine may still
// be logging while the test inspects what was written.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// mockDSM is the smallest server the connector's dial path accepts: it answers
// encrypt_channel with {"alive":1}, upgrades to TLS with a self-signed leaf, and
// answers every action turn (here: query_user_info, the mode-A reuse verb) with
// errno 0 - the success evidence auth.Reuse requires.
type mockDSM struct {
	ln   net.Listener
	cert tls.Certificate
}

func newMockDSM(t *testing.T) *mockDSM {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &mockDSM{ln: ln, cert: mockDSMCert(t)}
	go s.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *mockDSM) serve() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(conn)
	}
}

func (s *mockDSM) handle(conn net.Conn) {
	defer conn.Close()
	if _, err := sproto.ReadHeader(conn); err != nil {
		return
	}
	if _, err := sproto.NewDecoder(conn).Decode(); err != nil {
		return
	}
	if err := sproto.NewEncoder(conn).Encode(sproto.NewMap().Set("alive", sproto.Int(1))); err != nil {
		return
	}
	tc := tls.Server(conn, &tls.Config{
		Certificates: []tls.Certificate{s.cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err := tc.Handshake(); err != nil {
		return
	}
	for {
		if _, err := sproto.ReadHeader(tc); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(tc).Decode(); err != nil {
			return
		}
		if err := sproto.NewEncoder(tc).Encode(sproto.NewMap().Set("errno", sproto.Int(0))); err != nil {
			return
		}
	}
}

// legacyPin is the stored full-DER pin form the migration upgrades from.
func (s *mockDSM) legacyPin() string { return transport.Fingerprint(s.cert.Certificate[0]) }

// spkiPin is the form the migration must write to the keychain.
func (s *mockDSM) spkiPin(t *testing.T) string {
	t.Helper()
	pin, err := transport.FingerprintSPKI(s.cert.Certificate[0])
	if err != nil {
		t.Fatalf("FingerprintSPKI: %v", err)
	}
	return pin
}

// cfgFor points a test config at the mock server.
func (s *mockDSM) cfgFor() config.Config {
	addr := s.ln.Addr().(*net.TCPAddr)
	cfg := testConfig()
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = addr.Port
	return cfg
}

func mockDSMCert(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "synosync-mock-dsm"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(1<<31-1, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"synosync-mock-dsm"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// TestConnectorPinMigrationDoesNotHangDial reproduces the dial wedge: the
// legacy→SPKI keychain write used to run inline inside connect(), and go-keyring
// is not context-aware, so a backend blocked on an unanswerable prompt held the
// pool's dial path forever - connectAttemptTimeout (network stages only) and a
// pool Close both being powerless against it, with Reconnecting() false and the
// daemon silently wedged. The dial must now finish inside migratePinWait and
// hand the session to the pool, logging the missed migration non-fatally.
func TestConnectorPinMigrationDoesNotHangDial(t *testing.T) {
	srv := newMockDSM(t)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) }) // release the stalled keychain goroutine
	// setErr keeps the released write a no-op once the cleanup unblocks it, so the goroutine
	// outliving this test cannot mutate its fixtures (mirroring
	// TestPinMigrationRunHonoursContextCancel). The assertions below expect no stored pin
	// either way - the point is that the write never COMPLETED inside the dial.
	kr := &migSecrets{pin: srv.legacyPin(), session: "tok-123", block: block, setErr: errFake}

	restore := migratePinWait
	migratePinWait = 150 * time.Millisecond
	t.Cleanup(func() { migratePinWait = restore })

	var logBuf syncBuffer
	connect, _, err := NewConnector(srv.cfgFor(), kr, testClient(), slog.New(slog.NewTextHandler(&logBuf, nil)))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	// The parent context is generous: only the migration budget can end this
	// call quickly, and it must still be live afterwards.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	sess, err := connect(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	if sess == nil {
		t.Fatal("connect returned a nil session; the pool has nothing to use")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("connect took %v with a stalled keychain; the dial path must not block on it", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatalf("parent ctx expired (%v); connect hung inside the keychain phase", ctx.Err())
	}
	if got := logBuf.String(); !strings.Contains(got, "cert pin SPKI upgrade did not complete in time") {
		t.Errorf("stalled migration not reported non-fatally; log = %q", got)
	}
	if pins := kr.stored(); len(pins) != 0 {
		t.Errorf("stored pins = %v, want none (the write never completed)", pins)
	}

	// A second dial must not queue behind the still-stalled write either.
	start = time.Now()
	sess2, err := connect(ctx)
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	t.Cleanup(func() { _ = sess2.Close() })
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("second connect took %v; a wedged migration must not be re-waited", elapsed)
	}
}

// TestConnectorMigratesPinOnConnect confirms the healthy path still runs: a
// stored legacy pin is replaced by the presented leaf's SPKI form on the first
// successful reuse, exactly once, and later dials keep working.
func TestConnectorMigratesPinOnConnect(t *testing.T) {
	srv := newMockDSM(t)
	kr := &migSecrets{pin: srv.legacyPin(), session: "tok-123"}
	var logBuf syncBuffer
	connect, _, err := NewConnector(srv.cfgFor(), kr, testClient(), slog.New(slog.NewTextHandler(&logBuf, nil)))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sess, err := connect(ctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_ = sess.Close()
	want := srv.spkiPin(t)
	if pins := kr.stored(); len(pins) != 1 || pins[0] != want {
		t.Fatalf("stored pins = %v, want [%s]", pins, want)
	}
	if got := logBuf.String(); !strings.Contains(got, "certificate pin upgraded to SPKI") {
		t.Errorf("upgrade not logged; log = %q", got)
	}

	// The second dial verifies against the adopted SPKI pin and must not
	// re-store it.
	sess2, err := connect(ctx)
	if err != nil {
		t.Fatalf("second connect: %v", err)
	}
	_ = sess2.Close()
	if pins := kr.stored(); len(pins) != 1 {
		t.Fatalf("stored pins = %v, want the single first-dial write", pins)
	}
}

// TestConnectorPinMigrationRetriesAfterStoreFailure covers the tolerated-failure
// path end to end: a keychain that rejects the write must not fail the connect,
// and because the write is idempotent a later dial is free to retry it.
func TestConnectorPinMigrationRetriesAfterStoreFailure(t *testing.T) {
	srv := newMockDSM(t)
	kr := &migSecrets{pin: srv.legacyPin(), session: "tok-123", setErr: errFake}
	var logBuf syncBuffer
	connect, _, err := NewConnector(srv.cfgFor(), kr, testClient(), slog.New(slog.NewTextHandler(&logBuf, nil)))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 2 {
		sess, cerr := connect(ctx)
		if cerr != nil {
			t.Fatalf("connect %d with a failing keychain: %v", i, cerr)
		}
		_ = sess.Close()
	}
	if got := logBuf.String(); !strings.Contains(got, "upgrade cert pin to SPKI") {
		t.Errorf("store failure not logged; log = %q", got)
	}
	if pins := kr.stored(); len(pins) != 0 {
		t.Errorf("stored pins = %v, want none (every write failed)", pins)
	}
	// The retry itself, not just its absence of effect: `done = upgraded != ""` regressing to
	// an unconditional `done = true` would strand the daemon on the legacy full-DER pin for its
	// whole lifetime after ONE transient keychain failure, and every assertion above would
	// still hold (a failed write records nothing either way).
	if n := kr.attemptCount(); n != 2 {
		t.Errorf("SetPin attempts = %d, want 2 (a failed write must not mark the migration done)", n)
	}
}

// TestPinMigrationRunHonoursContextCancel pins the pool-Close contract directly:
// a cancelled context ends the wait immediately even though the keychain call
// itself cannot be interrupted, so daemon shutdown is never held by it.
func TestPinMigrationRunHonoursContextCancel(t *testing.T) {
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	// setErr keeps the released write a no-op once the cleanup unblocks it, so
	// the goroutine outliving this test cannot touch anything.
	kr := &migSecrets{block: block, setErr: errFake}

	restore := migratePinWait
	migratePinWait = time.Hour // only the cancel can end this wait
	t.Cleanup(func() { migratePinWait = restore })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var logBuf syncBuffer
	var adopted atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&pinMigration{}).run(ctx, kr, "nas.example:6690", "sha256:deadbeef", "spki-sha256:abc123",
			slog.New(slog.NewTextHandler(&logBuf, nil)), func(string) { adopted.Store(true) })
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after ctx cancel; the dial path is held by the keychain")
	}
	if adopted.Load() {
		t.Error("adopt called though the keychain write never completed")
	}
	if got := logBuf.String(); !strings.Contains(got, "cert pin SPKI upgrade abandoned") {
		t.Errorf("abandoned migration not reported; log = %q", got)
	}
}

// TestPinMigrationRunSkipsUnneededWork covers the cheap guards: a pin that needs
// no upgrade never reaches the keychain, and a completed migration is not
// repeated on later dials.
func TestPinMigrationRunSkipsUnneededWork(t *testing.T) {
	const server = "nas.example:6690"
	const spki = "spki-sha256:abc123"
	ctx := context.Background()

	cases := []struct {
		name string
		pin  string
		spki string
	}{
		{"already spki", spki, spki},
		{"no presented spki", "sha256:deadbeef", ""},
		{"no stored pin", "", spki},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kr := &migSecrets{}
			(&pinMigration{}).run(ctx, kr, server, tc.pin, tc.spki, nil, func(string) {
				t.Error("adopt called for a pin needing no upgrade")
			})
			if pins := kr.stored(); len(pins) != 0 {
				t.Fatalf("stored pins = %v, want none", pins)
			}
		})
	}

	t.Run("once per connector", func(t *testing.T) {
		kr := &migSecrets{}
		mig := &pinMigration{}
		var adopted int
		for range 3 {
			mig.run(ctx, kr, server, "sha256:deadbeef", spki, nil, func(string) { adopted++ })
		}
		if pins := kr.stored(); len(pins) != 1 {
			t.Fatalf("stored pins = %v, want exactly one write", pins)
		}
		if adopted != 1 {
			t.Fatalf("adopt called %d times, want 1", adopted)
		}
	})
}

// TestMigratePinWaitIsBounded guards the budget itself: it must be short enough
// that a stalled keychain cannot outlast a connect attempt (the pool would then
// be wedged for the whole attempt window anyway).
func TestMigratePinWaitIsBounded(t *testing.T) {
	if migratePinWait <= 0 {
		t.Fatalf("migratePinWait = %v, want a positive budget", migratePinWait)
	}
	if migratePinWait >= connectAttemptTimeout {
		t.Fatalf("migratePinWait (%v) >= connectAttemptTimeout (%v); the keychain phase could dominate a dial",
			migratePinWait, connectAttemptTimeout)
	}
}
