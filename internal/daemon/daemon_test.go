package daemon

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
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// openTestStore opens a fresh temp bbolt store for a test.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// listenTestSocket binds a temp AF_UNIX listener for a test.
func listenTestSocket(t *testing.T) (net.Listener, string) {
	t.Helper()
	path := filepath.Join(sockDir(t), "d.sock")
	l, err := ipc.Listen(path)
	if err != nil {
		t.Fatalf("ipc.Listen: %v", err)
	}
	return l, path
}

// tempSockPath returns an unused AF_UNIX socket path under a temp dir; Run binds
// it itself after taking the lock.
func tempSockPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(sockDir(t), "d.sock")
}

// sockDir returns a temp directory short enough to hold an AF_UNIX socket.
// sun_path caps a bound socket path at 104 bytes on macOS/BSD (108 on Linux),
// and t.TempDir() spends most of that budget before the socket name is even
// appended: macOS's per-user TMPDIR is already ~48 bytes of
// /var/folders/<2>/<30>/T/, onto which t.TempDir() adds the full test name, a
// random suffix and a per-call counter. A d.sock under it overruns the cap and
// every bind fails with EINVAL ("invalid argument") - which is how the macOS CI
// job went red across ipc, daemon and cmd. MkdirTemp with a short prefix keeps
// the path well inside the cap on every platform.
func sockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sy")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// fakePool is a poolState stand-in for tests.
type fakePool struct {
	paused       bool
	reconnecting bool
	workers      int
	pauseReason  error
}

func (p fakePool) Paused() bool       { return p.paused }
func (p fakePool) Reconnecting() bool { return p.reconnecting }
func (p fakePool) NumWorkers() int    { return p.workers }
func (p fakePool) PauseReason() error { return p.pauseReason }

func TestNewValidation(t *testing.T) {
	st := openTestStore(t)
	sock := tempSockPath(t)

	cases := []struct {
		name string
		deps Deps
	}{
		{"nil store", Deps{SockPath: sock, LockPath: "x"}},
		{"empty sock path", Deps{Store: st, LockPath: "x"}},
		{"empty lock path", Deps{Store: st, SockPath: sock}},
		{"nil connector and no run seam", Deps{Store: st, SockPath: sock, LockPath: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.deps); err == nil {
				t.Fatal("New: got nil error, want validation error")
			}
		})
	}
}

// TestNewWorkerCountFromConfig confirms the resolved worker count honours the config
// tunable (config.Limits.Workers) while an explicit Deps.NumWorkers still wins and a
// fully-unset count falls back to the package default.
func TestNewWorkerCountFromConfig(t *testing.T) {
	st := openTestStore(t)
	base := Deps{
		Store:    st,
		SockPath: tempSockPath(t),
		LockPath: filepath.Join(t.TempDir(), "l"),
		// New requires a Connector (or a run seam); it is never dialed in this test.
		Connector: func(context.Context) (*transport.Session, error) { return nil, nil },
	}

	cases := []struct {
		name       string
		depWorkers int
		cfgWorkers int
		want       int
	}{
		{"explicit dep wins", 2, 4, 2},
		{"config tunable used", 0, 4, 4},
		{"unset falls back to default", 0, 0, defaultNumWorkers},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := base
			d.NumWorkers = tc.depWorkers
			d.Config = config.Config{Limits: config.Limits{Workers: tc.cfgWorkers}}
			dm, err := New(d)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if dm.numWorkers != tc.want {
				t.Errorf("numWorkers = %d, want %d", dm.numWorkers, tc.want)
			}
		})
	}
}

func TestRunServesIPCAndShutsDownCleanly(t *testing.T) {
	st := openTestStore(t)
	sockPath := tempSockPath(t)
	lockPath := filepath.Join(t.TempDir(), "daemon.lock")

	started := make(chan struct{})
	deps := Deps{
		Store:    st,
		Config:   config.Config{SyncRoot: "/tmp/root"},
		SockPath: sockPath,
		LockPath: lockPath,
		runSync: func(ctx context.Context, d *Daemon) error {
			d.setPool(fakePool{workers: 3})
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	d, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx) }()

	<-started

	// IPC is being served: a dialed call to an unknown method reaches the server
	// and comes back as an error (proving the accept loop is live).
	c, err := ipc.Dial(sockPath)
	if err != nil {
		t.Fatalf("ipc.Dial: %v", err)
	}
	if err := c.Call("does-not-exist", nil, nil); err == nil {
		t.Fatal("Call unknown method: got nil error, want unknown-method error")
	}
	_ = c.Close()

	// The lock is held while Run is active.
	if _, err := acquireLock(lockPath); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("acquireLock during Run: got %v, want ErrAlreadyRunning", err)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run clean shutdown: got %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	// After shutdown the lock is released and re-acquirable.
	release, err := acquireLock(lockPath)
	if err != nil {
		t.Fatalf("acquireLock after shutdown: %v", err)
	}
	_ = release()
}

func TestRunSurfacesTerminalSyncError(t *testing.T) {
	st := openTestStore(t)
	lockPath := filepath.Join(t.TempDir(), "daemon.lock")

	deps := Deps{
		Store:    st,
		SockPath: tempSockPath(t),
		LockPath: lockPath,
		runSync: func(ctx context.Context, d *Daemon) error {
			return transport.ErrPoolPaused
		},
	}
	d, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = d.Run(context.Background())
	if !errors.Is(err, transport.ErrPoolPaused) {
		t.Fatalf("Run terminal error: got %v, want ErrPoolPaused", err)
	}
}

// TestDaemonRunLogsTerminalStopReason pins the stop-path
// observability: a terminal sync error is logged as an Error record with a
// classification (session vanished / pin mismatch / other) BEFORE Run returns it.
// Under the Windows SCM stderr is discarded, so without this record the rotating
// log stays silent on why the service exits and crash-loops every restart
// interval. A clean ctx-driven shutdown must log no such record.
func TestDaemonRunLogsTerminalStopReason(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"session-vanished", fmt.Errorf("engine: %w", transport.ErrPoolPaused), `reason="session vanished"`},
		{"pin-mismatch", fmt.Errorf("daemon: connect: %w", transport.ErrPinMismatch), `reason="pin mismatch"`},
		{"client-version", fmt.Errorf("engine: %w", transport.ErrPoolClientRejected), `reason="client version rejected"`},
		{"other", errors.New("store exploded"), "reason=other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			d, err := New(Deps{
				Store:    openTestStore(t),
				SockPath: tempSockPath(t),
				LockPath: filepath.Join(t.TempDir(), "daemon.lock"),
				Logger:   slog.New(slog.NewTextHandler(&logBuf, nil)),
				runSync:  func(ctx context.Context, d *Daemon) error { return tc.err },
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if rerr := d.Run(context.Background()); !errors.Is(rerr, tc.err) {
				t.Fatalf("Run: got %v, want %v", rerr, tc.err)
			}
			got := logBuf.String()
			if !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "sync loop stopped on terminal error") {
				t.Errorf("log = %q, want an ERROR stop record", got)
			}
			if !strings.Contains(got, tc.wantReason) {
				t.Errorf("log = %q, want classification %q", got, tc.wantReason)
			}
		})
	}

	t.Run("clean-shutdown-logs-nothing", func(t *testing.T) {
		var logBuf bytes.Buffer
		d, err := New(Deps{
			Store:    openTestStore(t),
			SockPath: tempSockPath(t),
			LockPath: filepath.Join(t.TempDir(), "daemon.lock"),
			Logger:   slog.New(slog.NewTextHandler(&logBuf, nil)),
			runSync: func(ctx context.Context, d *Daemon) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		runErr := make(chan error, 1)
		go func() { runErr <- d.Run(ctx) }()
		cancel()
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("Run clean shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after ctx cancel")
		}
		if strings.Contains(logBuf.String(), "sync loop stopped on terminal error") {
			t.Errorf("log = %q, want no terminal-stop record on a clean shutdown", logBuf.String())
		}
	})
}

func TestRunSecondInstanceFails(t *testing.T) {
	st := openTestStore(t)
	lockPath := filepath.Join(t.TempDir(), "daemon.lock")

	started := make(chan struct{})
	first, err := New(Deps{
		Store:    st,
		SockPath: tempSockPath(t),
		LockPath: lockPath,
		runSync: func(ctx context.Context, d *Daemon) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("New first: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Run(ctx) }()
	<-started

	second, err := New(Deps{
		Store:    openTestStore(t),
		SockPath: tempSockPath(t),
		LockPath: lockPath,
		runSync:  func(ctx context.Context, d *Daemon) error { return nil },
	})
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	if err := second.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run: got %v, want ErrAlreadyRunning", err)
	}

	cancel()
	if err := <-firstDone; err != nil {
		t.Fatalf("first Run shutdown: %v", err)
	}
}

func TestDefaultWatcherFactory(t *testing.T) {
	f := defaultWatcherFactory(slog.New(slog.NewTextHandler(io.Discard, nil)), 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := f(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("watcher factory: %v", err)
	}
	if ch == nil {
		t.Fatal("watcher factory: nil wake channel")
	}
}

// TestDefaultRunSyncPoolStartError exercises the production run seam far enough to
// cover the pool bring-up without a live server: a Connector that always fails
// makes pool.Start fail before the engine is built. A transient dial failure is
// not terminal - the loop retries under backoff - so the test cancels the context
// from inside the connector to confirm the retry loop exits cleanly on ctx
// cancellation (and that the connector was actually invoked, i.e. a Start was
// attempted).
func TestDefaultRunSyncPoolStartError(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	d := &Daemon{
		store:      st,
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			calls++
			// Cancel after the first failed bring-up so the retry loop stops via ctx
			// rather than spinning forever against the permanently-failing connector.
			cancel()
			return nil, errors.New("dial refused")
		},
	}
	err := defaultRunSync(ctx, d)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("defaultRunSync: got %v, want context.Canceled", err)
	}
	if calls == 0 {
		t.Fatal("defaultRunSync: connector never invoked (no Start attempted)")
	}
}

// TestDefaultRunSyncPinMismatchTerminal confirms a certificate pin mismatch during
// bring-up stops the daemon for re-pairing rather than retrying forever: the pool
// pauses on the mismatch and surfaces ErrPoolPaused wrapping ErrPinMismatch, which
// defaultRunSync returns terminally after a single Start attempt.
func TestDefaultRunSyncPinMismatchTerminal(t *testing.T) {
	st := openTestStore(t)
	var calls int
	d := &Daemon{
		store:      st,
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			calls++
			return nil, fmt.Errorf("daemon: connect: %w", transport.ErrPinMismatch)
		},
	}
	err := defaultRunSync(context.Background(), d)
	if !errors.Is(err, transport.ErrPinMismatch) {
		t.Fatalf("defaultRunSync: got %v, want it to wrap ErrPinMismatch", err)
	}
	if calls != 1 {
		t.Fatalf("defaultRunSync: connector invoked %d times, want 1 (no retry on pin mismatch)", calls)
	}
}

// TestDefaultRunSyncClientVersionTerminal confirms a rejected client_version (20481,
// auth.ErrClientVersion) during bring-up stops the daemon for a client upgrade rather
// than re-dialing the refused binary forever: the pool pauses on the rejection and
// surfaces ErrPoolClientRejected wrapping auth.ErrClientVersion, which defaultRunSync
// returns terminally after a single Start attempt, classified as "client version
// rejected" in the stop-path log.
func TestDefaultRunSyncClientVersionTerminal(t *testing.T) {
	var logBuf bytes.Buffer
	var calls int
	d := &Daemon{
		store:      openTestStore(t),
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(&logBuf, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			calls++
			return nil, fmt.Errorf("daemon: reuse session: %w", auth.ErrClientVersion)
		},
	}
	err := defaultRunSync(context.Background(), d)
	if !errors.Is(err, transport.ErrPoolClientRejected) {
		t.Fatalf("defaultRunSync: got %v, want it to wrap ErrPoolClientRejected", err)
	}
	if !errors.Is(err, auth.ErrClientVersion) {
		t.Fatalf("defaultRunSync: got %v, want it to wrap auth.ErrClientVersion", err)
	}
	if errors.Is(err, transport.ErrPoolPaused) {
		t.Fatalf("defaultRunSync: got %v, must not read as the re-pair ErrPoolPaused", err)
	}
	if calls != 1 {
		t.Fatalf("defaultRunSync: connector invoked %d times, want 1 (no retry on rejected client version)", calls)
	}
	if got := logBuf.String(); !strings.Contains(got, `reason="client version rejected"`) {
		t.Errorf("log = %q, want client-version classification", got)
	}
}

// TestDefaultRunSyncLogsTerminalPoolStart pins the bring-up
// half of the stop-path observability: a TERMINAL pool.Start failure (here a pin
// mismatch, surfaced as ErrPoolPaused wrapping ErrPinMismatch) is logged at Error
// with its classification before defaultRunSync returns it - previously only
// transient bring-up failures were Warned, so a supervisor-restarted service
// died on the same Start every few seconds with no log line naming the cause.
func TestDefaultRunSyncLogsTerminalPoolStart(t *testing.T) {
	var logBuf bytes.Buffer
	d := &Daemon{
		store:      openTestStore(t),
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(&logBuf, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			return nil, fmt.Errorf("daemon: connect: %w", transport.ErrPinMismatch)
		},
	}
	if err := defaultRunSync(context.Background(), d); !errors.Is(err, transport.ErrPinMismatch) {
		t.Fatalf("defaultRunSync: got %v, want it to wrap ErrPinMismatch", err)
	}
	got := logBuf.String()
	if !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "connection pool start failed terminally") {
		t.Errorf("log = %q, want an ERROR terminal bring-up record", got)
	}
	if !strings.Contains(got, `reason="pin mismatch"`) {
		t.Errorf("log = %q, want pin-mismatch classification", got)
	}
}

// TestDefaultRunSyncRetriesTransientStart confirms a transient pool.Start failure
// is retried rather than killing the daemon: the connector fails the first
// bring-up, then cancels the context on the second attempt so the loop exits via
// ctx. Observing a second invocation proves the first failure did not return.
func TestDefaultRunSyncRetriesTransientStart(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	var starts int
	d := &Daemon{
		store:      st,
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			starts++
			// The event-connection dial is the first connect of each Start; cancel on
			// the second Start's first dial so the loop terminates promptly.
			if starts >= 2 {
				cancel()
			}
			return nil, errors.New("dial refused")
		},
	}
	// One backoff sleep (~base delay) separates the two Starts; well under a second.
	if err := defaultRunSync(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatalf("defaultRunSync: got %v, want context.Canceled", err)
	}
	if starts < 2 {
		t.Fatalf("defaultRunSync: connector invoked %d times, want >= 2 (no retry after first failure)", starts)
	}
}

// TestDefaultRunSyncRetriesAttemptTimeout pins the bring-up
// classification: a pool.Start failure carrying the connector's per-attempt
// deadline (a wrapped context.DeadlineExceeded, parent ctx still live) is
// transient - the bring-up backoff retries it - not a terminal error that would
// kill the daemon while the server is merely hung.
func TestDefaultRunSyncRetriesAttemptTimeout(t *testing.T) {
	st := openTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	var starts int
	d := &Daemon{
		store:      st,
		numWorkers: 2,
		log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		connector: func(ctx context.Context) (*transport.Session, error) {
			starts++
			if starts >= 2 {
				cancel()
			}
			return nil, fmt.Errorf("daemon: connect nas.example:6690: %w", context.DeadlineExceeded)
		},
	}
	if err := defaultRunSync(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatalf("defaultRunSync: got %v, want context.Canceled", err)
	}
	if starts < 2 {
		t.Fatalf("defaultRunSync: connector invoked %d times, want >= 2 (attempt timeout read as terminal)", starts)
	}
}
