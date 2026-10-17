package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/backoff"
)

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

// serveTest starts a Server on a fresh temp socket and returns its path plus a
// cleanup that cancels Serve and waits for it to return.
func serveTest(t *testing.T, configure func(*Server)) string {
	t.Helper()
	srv := NewServer()
	configure(srv)
	return serveServer(t, srv)
}

// serveServer runs an already-constructed Server on a fresh temp socket,
// registering the cancel-and-wait cleanup; the injected-logger tests build the
// Server themselves via NewServerWithLogger.
func serveServer(t *testing.T, srv *Server) string {
	t.Helper()
	sock := filepath.Join(sockDir(t), "d.sock")
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(ctx, l); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return sock
}

func TestRoundTrip(t *testing.T) {
	sock := serveTest(t, func(s *Server) {
		s.Handle("echo", func(_ context.Context, params json.RawMessage) (any, error) {
			var in struct {
				Msg string `json:"msg"`
			}
			if err := json.Unmarshal(params, &in); err != nil {
				return nil, err
			}
			return map[string]string{"reply": in.Msg + "!"}, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var out struct {
		Reply string `json:"reply"`
	}
	if err := c.Call("echo", map[string]string{"msg": "hi"}, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.Reply != "hi!" {
		t.Fatalf("reply = %q, want %q", out.Reply, "hi!")
	}
}

func TestRequestSizeLimited(t *testing.T) {
	var called int32
	sock := serveTest(t, func(s *Server) {
		s.Handle("sink", func(context.Context, json.RawMessage) (any, error) {
			atomic.AddInt32(&called, 1)
			return map[string]string{"ok": "yes"}, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	// A request whose params far exceed maxRequestBytes (1 MiB) must be refused
	// before dispatch: the server's LimitReader truncates the read, Decode fails,
	// and the connection closes without ever invoking the handler.
	huge := strings.Repeat("a", (1<<20)+4096)
	var out map[string]string
	if err := c.Call("sink", map[string]string{"blob": huge}, &out); err == nil {
		t.Fatal("Call with oversized request = nil error, want failure")
	}
	if n := atomic.LoadInt32(&called); n != 0 {
		t.Fatalf("handler invoked %d times for oversized request, want 0", n)
	}
}

func TestResponseSizeLimited(t *testing.T) {
	sock := serveTest(t, func(s *Server) {
		s.Handle("flood", func(context.Context, json.RawMessage) (any, error) {
			// A reply whose result far exceeds maxResponseBytes (1 MiB).
			return map[string]string{"blob": strings.Repeat("a", (1<<20)+4096)}, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	// The client's LimitReader truncates the oversized reply, so Decode fails
	// rather than the client buffering the whole unbounded response.
	var out map[string]string
	if err := c.Call("flood", nil, &out); err == nil {
		t.Fatal("Call with oversized response = nil error, want failure")
	}
}

func TestInFlightConnsBounded(t *testing.T) {
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	release := make(chan struct{})
	sock := serveTest(t, func(s *Server) {
		s.Handle("block", func(context.Context, json.RawMessage) (any, error) {
			mu.Lock()
			inFlight++
			if inFlight > peak {
				peak = inFlight
			}
			mu.Unlock()
			<-release // hold the connection open while concurrent clients pile up
			mu.Lock()
			inFlight--
			mu.Unlock()
			return map[string]string{"ok": "yes"}, nil
		})
	})

	const clients = maxInFlightConns * 3
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := Dial(sock)
			if err != nil {
				return
			}
			defer c.Close()
			var out map[string]string
			_ = c.Call("block", nil, &out)
		}()
	}

	// Wait until the semaphore is saturated (the cap's worth of handlers are blocked
	// concurrently), then release them all.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		p := peak
		mu.Unlock()
		if p >= maxInFlightConns || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if peak > maxInFlightConns {
		t.Fatalf("concurrent in-flight handlers peaked at %d, want <= %d", peak, maxInFlightConns)
	}
	if peak == 0 {
		t.Fatal("no handlers ran; test did not exercise the accept path")
	}
}

func TestNoParamsAndStatusReport(t *testing.T) {
	want := StatusReport{
		Server:     "host:6690",
		PendingOps: 3,
		// Stuck pins the additive stuck-op counter through
		// the JSON round-trip.
		Stuck: 5,
		Views: []ViewStatus{
			{ID: 1, Name: "team", LastSyncID: 42, InitialSyncDone: true, Conflicts: 2},
		},
		// Connecting: true pins the additive third connection state through the
		// JSON round-trip (the struct compare below covers it).
		Connection: ConnState{Connecting: true, Paused: false, NumWorkers: 4},
	}
	sock := serveTest(t, func(s *Server) {
		s.Handle("status", func(_ context.Context, params json.RawMessage) (any, error) {
			if params != nil {
				t.Errorf("expected nil params, got %s", params)
			}
			return want, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var got StatusReport
	if err := c.Call("status", nil, &got); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got.Server != want.Server || got.PendingOps != want.PendingOps || got.Stuck != want.Stuck ||
		got.Connection != want.Connection || len(got.Views) != 1 || !reflect.DeepEqual(got.Views[0], want.Views[0]) {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
}

func TestErrorPropagation(t *testing.T) {
	sock := serveTest(t, func(s *Server) {
		s.Handle("boom", func(_ context.Context, params json.RawMessage) (any, error) {
			return nil, errors.New("kaboom")
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	err = c.Call("boom", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("Call error = %v, want containing %q", err, "kaboom")
	}
}

func TestHandlerPanicRecovered(t *testing.T) {
	var goodCalled int32
	sock := serveTest(t, func(s *Server) {
		s.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
			panic("handler exploded")
		})
		s.Handle("good", func(context.Context, json.RawMessage) (any, error) {
			atomic.AddInt32(&goodCalled, 1)
			return map[string]string{"ok": "yes"}, nil
		})
	})

	// A panicking handler must surface as a reply error to the caller, not crash
	// the daemon process: each Call is a fresh connection, so if the panic took
	// down the process the second Call would fail to dial/serve.
	c1, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c1.Close()
	err = c1.Call("boom", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Call after panic = %v, want a panic error", err)
	}

	// The Accept loop must still serve subsequent requests.
	c2, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial after panic: %v", err)
	}
	defer c2.Close()
	var out map[string]string
	if err := c2.Call("good", nil, &out); err != nil {
		t.Fatalf("Call after panic: %v", err)
	}
	if out["ok"] != "yes" {
		t.Fatalf("good reply = %v, want ok=yes", out)
	}
	if n := atomic.LoadInt32(&goodCalled); n != 1 {
		t.Fatalf("good handler called %d times, want 1", n)
	}
}

func TestHandlerNotFound(t *testing.T) {
	sock := serveTest(t, func(s *Server) {})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	err = c.Call("nope", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown method") {
		t.Fatalf("Call error = %v, want containing %q", err, "unknown method")
	}
}

func TestSocketPerms0600(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix permission bits are not meaningful on windows")
	}
	sock := filepath.Join(sockDir(t), "perm.sock")
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer l.Close()

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perm = %o, want 600", perm)
	}
}

// errListener is a net.Listener whose Accept always fails with err, used to
// drive Serve's Accept-error paths. Close records that it ran so a test can
// assert the listener-closing goroutine did not leak past Serve.
type errListener struct {
	err    error
	closed chan struct{}
	once   sync.Once
}

func (l *errListener) Accept() (net.Conn, error) { return nil, l.err }
func (l *errListener) Close() error              { l.once.Do(func() { close(l.closed) }); return nil }
func (l *errListener) Addr() net.Addr            { return fakeAddr{} }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// flakyListener fails its first `failures` Accept calls with a transient error,
// then delegates to the wrapped real listener.
type flakyListener struct {
	net.Listener
	failures int32
}

func (l *flakyListener) Accept() (net.Conn, error) {
	if atomic.AddInt32(&l.failures, -1) >= 0 {
		return nil, errors.New("accept: too many open files")
	}
	return l.Listener.Accept()
}

// fastBackoff is a millisecond-scale deterministic delay sequence so retry
// tests do not sleep for the production half-second base.
func fastBackoff() *backoff.Backoff {
	return &backoff.Backoff{Base: time.Millisecond, Max: time.Millisecond, Factor: 1, Jitter: 0}
}

func TestServeReturnsAcceptError(t *testing.T) {
	// A listener closed underneath a live Serve (the net.ErrClosed class) is the
	// one terminal Accept failure: nothing more can ever be accepted, so Serve
	// must surface it rather than retry.
	l := &errListener{err: net.ErrClosed, closed: make(chan struct{})}
	srv := NewServer()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "accept") {
			t.Fatalf("Serve error = %v, want an accept error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return on a closed listener")
	}

	// Serve's defer must cancel its internal context so the listener-closing
	// goroutine unblocks and closes l rather than leaking until the parent ctx ends.
	select {
	case <-l.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve leaked its listener-close goroutine (listener never closed)")
	}
}

func TestServeRetriesTransientAcceptError(t *testing.T) {
	sock := filepath.Join(sockDir(t), "flaky.sock")
	inner, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	l := &flakyListener{Listener: inner, failures: 3}

	srv := NewServer()
	srv.newBackoff = fastBackoff
	srv.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return map[string]string{"ok": "yes"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after ctx cancel")
		}
	})

	// The first Accept calls fail transiently (fd-pressure style). Serve must log
	// and retry rather than return, so this call - a fresh connection queued on
	// the still-bound socket - is eventually accepted and served.
	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	var out map[string]string
	if err := c.Call("ping", nil, &out); err != nil {
		t.Fatalf("Call after transient accept failures: %v", err)
	}
	if out["ok"] != "yes" {
		t.Fatalf("reply = %v, want ok=yes", out)
	}
	if n := atomic.LoadInt32(&l.failures); n >= 0 {
		t.Fatalf("transient accept path not exercised (remaining failures = %d)", n)
	}
}

func TestServeTransientRetryCtxCancelCleanExit(t *testing.T) {
	// An always-transiently-failing listener with the production backoff: cancel
	// lands inside the retry wait, and Serve must exit clean (nil), not surface
	// the transient error.
	l := &errListener{err: errors.New("accept: resource temporarily unavailable"), closed: make(chan struct{})}
	srv := NewServer()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve after cancel during retry = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after ctx cancel during accept retry")
	}
	select {
	case <-l.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("listener not closed on Serve exit")
	}
}

func TestStaleSocketReplaced(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("named-pipe Listen does not touch the filesystem; no stale file to replace")
	}
	sock := filepath.Join(sockDir(t), "stale.sock")

	// Simulate a socket file left behind by a previous (crashed) daemon. net's
	// UnixListener unlinks on Close, so a leftover regular file at the path
	// exercises the same os.Remove path Listen relies on.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale file: %v", err)
	}

	// A fresh Listen must remove the stale file and succeed.
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("re-listen over stale socket: %v", err)
	}
	defer l.Close()
}

func TestStatusReportPausedRoundTrip(t *testing.T) {
	want := StatusReport{
		Server:     "host:6690",
		PendingOps: 1,
		Views: []ViewStatus{
			{ID: 7, Name: "team", LastSyncID: 5, InitialSyncDone: true, Conflicts: 1},
		},
		Connection: ConnState{Paused: false, NumWorkers: 3},
		Paused:     true,
	}

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// The user-paused field must serialise under "paused", distinct from the
	// nested connection "paused".
	if !strings.Contains(string(b), `"paused":true`) {
		t.Fatalf("marshalled status missing user paused flag: %s", b)
	}

	var got StatusReport
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.Paused != want.Paused {
		t.Fatalf("Paused = %v, want %v", got.Paused, want.Paused)
	}
	if got.Connection != want.Connection {
		t.Fatalf("Connection = %+v, want %+v", got.Connection, want.Connection)
	}
	if got.Server != want.Server || got.PendingOps != want.PendingOps ||
		len(got.Views) != 1 || !reflect.DeepEqual(got.Views[0], want.Views[0]) {
		t.Fatalf("status = %+v, want %+v", got, want)
	}
}

func TestConflictReportRoundTrip(t *testing.T) {
	want := ConflictReport{
		Conflicts: []ConflictEntry{
			{
				ViewID:        7,
				ViewName:      "team",
				CanonicalPath: "docs/report.txt",
				CopyPath:      "docs/report_host_Jun-12-101500-2026_Conflict.txt",
				DetectedAt:    1749720900,
			},
			{
				ViewID:        9,
				ViewName:      "photos",
				CanonicalPath: "a/b.jpg",
				CopyPath:      "a/b_host_Jun-12-101501-2026_Conflict.jpg",
				DetectedAt:    1749720901,
			},
		},
		Total:     5,
		Truncated: true,
	}

	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// The truncation fields are additive in the wire contract;
	// pin their JSON keys so a rename can't silently desync producer and consumer.
	for _, key := range []string{`"total":5`, `"truncated":true`} {
		if !strings.Contains(string(b), key) {
			t.Fatalf("marshalled report missing %s: %s", key, b)
		}
	}

	var got ConflictReport
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Conflicts) != len(want.Conflicts) {
		t.Fatalf("Conflicts len = %d, want %d", len(got.Conflicts), len(want.Conflicts))
	}
	for i := range want.Conflicts {
		if got.Conflicts[i] != want.Conflicts[i] {
			t.Fatalf("Conflicts[%d] = %+v, want %+v", i, got.Conflicts[i], want.Conflicts[i])
		}
	}
	if got.Total != want.Total || got.Truncated != want.Truncated {
		t.Fatalf("Total/Truncated = %d/%v, want %d/%v", got.Total, got.Truncated, want.Total, want.Truncated)
	}
}

func TestConflictReportEmptyRoundTrip(t *testing.T) {
	var want ConflictReport
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got ConflictReport
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Conflicts) != 0 {
		t.Fatalf("empty ConflictReport decoded %d entries", len(got.Conflicts))
	}
}

// recordingHandler is a slog.Handler that collects every record it handles, so
// tests can assert the Server's diagnostics reach an injected logger (the
// daemon's rotating file) rather than slog.Default().
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r)
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// find returns the first collected record at level whose message contains msg.
func (h *recordingHandler) find(level slog.Level, msg string) (slog.Record, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == level && strings.Contains(r.Message, msg) {
			return r, true
		}
	}
	return slog.Record{}, false
}

func TestInjectedLoggerReceivesHandlerPanicError(t *testing.T) {
	rec := &recordingHandler{}
	srv := NewServerWithLogger(slog.New(rec))
	srv.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
		panic("handler exploded")
	})
	sock := serveServer(t, srv)

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	if err := c.Call("boom", nil, nil); err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("Call = %v, want a panic error", err)
	}

	// dispatch logs the Error before serveConn writes the reply, so by the time
	// Call returned the record must have been handled.
	if _, ok := rec.find(slog.LevelError, "handler panicked"); !ok {
		t.Fatalf("handler-panic Error did not reach the injected logger (records: %+v)", rec.records)
	}
}

func TestInjectedLoggerReceivesAcceptRetryWarn(t *testing.T) {
	sock := filepath.Join(sockDir(t), "flaky.sock")
	inner, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	l := &flakyListener{Listener: inner, failures: 1}

	rec := &recordingHandler{}
	srv := NewServerWithLogger(slog.New(rec))
	srv.newBackoff = fastBackoff
	srv.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return map[string]string{"ok": "yes"}, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return after ctx cancel")
		}
	})

	// The served round-trip proves the transient accept failure was retried, so
	// the Warn it logged must already be in the recorder.
	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	var out map[string]string
	if err := c.Call("ping", nil, &out); err != nil {
		t.Fatalf("Call after transient accept failure: %v", err)
	}
	if _, ok := rec.find(slog.LevelWarn, "accept failed"); !ok {
		t.Fatalf("accept-retry Warn did not reach the injected logger (records: %+v)", rec.records)
	}
}

// TestMalformedRequestGetsAnError pins that the daemon ANSWERS a request it cannot decode.
//
// Hanging up without a word left the client reporting "read response: EOF", which reads as a
// daemon that died mid-call rather than as a request it refused - and an unknown METHOD already
// got a proper error, so a malformed request was the one input whose rejection was invisible.
func TestMalformedRequestGetsAnError(t *testing.T) {
	sock := serveTest(t, func(s *Server) {})

	// The platform Dial, then its raw connection: the point is to send bytes the client
	// would never produce.
	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	conn := c.conn

	if _, err := conn.Write([]byte("this is not json\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	var resp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("no reply to a malformed request: %v", err)
	}
	if !strings.Contains(resp.Error, "malformed request") {
		t.Errorf("reply error = %q, want it to name the malformed request", resp.Error)
	}
}
