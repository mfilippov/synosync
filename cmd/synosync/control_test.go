package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
)

// stubControlSocket starts a real in-process IPC server on a temp socket with the
// given method handlers, points resolveDaemonPaths at its directory, and
// registers cleanup. It is the multi-method analogue of stubDaemonSocket used by
// the live-control subcommand tests.
func stubControlSocket(t *testing.T, handlers map[string]ipc.Handler) {
	t.Helper()
	dir := sockDir(t)
	paths := config.Paths{StateDir: dir}

	ln, err := ipc.Listen(paths.DaemonSock())
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := ipc.NewServer()
	for method, h := range handlers {
		srv.Handle(method, h)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) { return paths, nil }
	t.Cleanup(func() { resolveDaemonPaths = orig })
}

// pointAtEmptyDir points resolveDaemonPaths at a temp dir with nothing in it: no
// socket AND no state database, i.e. a FRESH install that has never been paired.
// A dial fails with ENOENT and the CLI reports the pairing step.
func pointAtEmptyDir(t *testing.T) {
	t.Helper()
	pointAtDir(t, sockDir(t))
}

// pointAtPairedDir points resolveDaemonPaths at a temp dir holding a state
// database but no socket: an install that HAS been set up whose daemon is simply
// down. The two cases used to be one, because the CLI answered both with "start
// it with `synosync daemon`" - advice that sends a fresh install to a command
// which replies "no config found; run `synosync pair` first".
// pointAtPairedDir stands up a state directory that is genuinely PAIRED.
//
// It used to write a zero-byte state.db, which made the fixture's name a lie: the "not paired
// yet" advice keys on an identity ever having been recorded, not on the file existing, because
// opening the store is something read-only commands do. An empty file is exactly the shape that
// produces - so these tests were asserting the not-running hint against an install that is not
// paired, and passed only because the check reached the developer's real profile instead.
func pointAtPairedDir(t *testing.T) {
	t.Helper()
	dir := sockDir(t)
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatalf("seed state db: %v", err)
	}
	cfg := config.Default()
	cfg.DeviceUUID = "0123456789abcdef0123456789abcdef"
	if err := st.PutConfig(cfg); err != nil {
		t.Fatalf("seed config: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close seeded store: %v", err)
	}
	pointAtDir(t, dir)
}

func pointAtDir(t *testing.T, dir string) {
	t.Helper()
	orig := resolveDaemonPaths
	resolveDaemonPaths = func() (config.Paths, error) {
		return config.Paths{StateDir: dir}, nil
	}
	t.Cleanup(func() { resolveDaemonPaths = orig })
}

// ackHandler records that method was called and returns a ControlAck.
func ackHandler(called *bool) ipc.Handler {
	return func(context.Context, json.RawMessage) (any, error) {
		*called = true
		return ipc.ControlAck{OK: true}, nil
	}
}

func TestSyncTriggers(t *testing.T) {
	for _, args := range [][]string{{"sync"}, {"sync", "now"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var called bool
			stubControlSocket(t, map[string]ipc.Handler{"sync": ackHandler(&called)})

			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if !called {
				t.Error("sync handler not called")
			}
			if !strings.Contains(stdout.String(), "sync triggered") {
				t.Errorf("stdout = %q, want \"sync triggered\"", stdout.String())
			}
		})
	}
}

func TestSyncBadArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"sync", "bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `"bogus"`) {
		t.Errorf("stderr = %q, want the rejected argument named", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage:\n  synosync sync [now]") {
		t.Errorf("stderr = %q, want the invocation form", stderr.String())
	}
}

func TestSyncDaemonNotRunning(t *testing.T) {
	pointAtPairedDir(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"sync"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not running") {
		t.Errorf("stderr = %q, want not-running message", stderr.String())
	}
}

func TestPauseResume(t *testing.T) {
	cases := []struct {
		verb string
		want string
	}{
		{"pause", "sync paused"},
		{"resume", "sync resumed"},
	}
	for _, tc := range cases {
		t.Run(tc.verb, func(t *testing.T) {
			var called bool
			stubControlSocket(t, map[string]ipc.Handler{tc.verb: ackHandler(&called)})

			var stdout, stderr bytes.Buffer
			if code := run([]string{tc.verb}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if !called {
				t.Errorf("%s handler not called", tc.verb)
			}
			if !strings.Contains(stdout.String(), tc.want) {
				t.Errorf("stdout = %q, want %q", stdout.String(), tc.want)
			}
		})
	}
}

func TestPauseResumeDaemonNotRunning(t *testing.T) {
	for _, verb := range []string{"pause", "resume"} {
		t.Run(verb, func(t *testing.T) {
			pointAtPairedDir(t)
			var stdout, stderr bytes.Buffer
			if code := run([]string{verb}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), "not running") {
				t.Errorf("stderr = %q, want not-running message", stderr.String())
			}
		})
	}
}

func TestConflictsList(t *testing.T) {
	report := ipc.ConflictReport{Conflicts: []ipc.ConflictEntry{
		{
			ViewID:        1,
			ViewName:      "team",
			CanonicalPath: "docs/report.txt",
			CopyPath:      "docs/report_host_Jun-12-120000-2026_Conflict.txt",
			DetectedAt:    1_700_000_000,
		},
	}}
	stubControlSocket(t, map[string]ipc.Handler{
		"conflicts": func(context.Context, json.RawMessage) (any, error) { return report, nil },
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"conflicts"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{
		"1 conflict(s):",
		"[1] team",
		"docs/report.txt",
		"docs/report_host_Jun-12-120000-2026_Conflict.txt",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout missing %q\nfull output:\n%s", want, out)
		}
	}
}

// TestConflictsListTruncated confirms the CLI renders a daemon-truncated report
// honestly: the header says how many of the total are shown (the daemon caps the
// list, newest first, so the report fits the IPC reply limit).
func TestConflictsListTruncated(t *testing.T) {
	report := ipc.ConflictReport{
		Conflicts: []ipc.ConflictEntry{
			{ViewID: 1, ViewName: "team", CanonicalPath: "b.txt", CopyPath: "b_Conflict.txt", DetectedAt: 200},
			{ViewID: 1, ViewName: "team", CanonicalPath: "a.txt", CopyPath: "a_Conflict.txt", DetectedAt: 100},
		},
		Total:     5,
		Truncated: true,
	}
	stubControlSocket(t, map[string]ipc.Handler{
		"conflicts": func(context.Context, json.RawMessage) (any, error) { return report, nil },
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"conflicts"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "5 conflict(s), showing the newest 2:") {
		t.Errorf("stdout missing truncation header\nfull output:\n%s", out)
	}
	for _, want := range []string{"a.txt", "b.txt"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout missing %q\nfull output:\n%s", want, out)
		}
	}
}

func TestConflictsEmpty(t *testing.T) {
	stubControlSocket(t, map[string]ipc.Handler{
		"conflicts": func(context.Context, json.RawMessage) (any, error) { return ipc.ConflictReport{}, nil },
	})

	var stdout, stderr bytes.Buffer
	if code := run([]string{"conflicts"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "No conflicts.") {
		t.Errorf("stdout = %q, want no-conflicts line", stdout.String())
	}
}

func TestConflictsDaemonNotRunning(t *testing.T) {
	pointAtPairedDir(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"conflicts"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not running") {
		t.Errorf("stderr = %q, want not-running message", stderr.String())
	}
}

// errHandler returns a handler that always fails, so the daemon dials fine but
// the control method itself errors - exercising each verb's post-dial Call-error
// branch (distinct from the daemon-not-running dial failure).
func errHandler() ipc.Handler {
	return func(context.Context, json.RawMessage) (any, error) {
		return nil, errors.New("boom")
	}
}

func TestControlVerbsCallError(t *testing.T) {
	for _, verb := range []string{"sync", "pause", "resume", "conflicts"} {
		t.Run(verb, func(t *testing.T) {
			stubControlSocket(t, map[string]ipc.Handler{verb: errHandler()})

			var stdout, stderr bytes.Buffer
			if code := run([]string{verb}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1 (stderr: %q)", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), "synosync: "+verb+":") {
				t.Errorf("stderr = %q, want %q prefix", stderr.String(), "synosync: "+verb+":")
			}
			if !strings.Contains(stderr.String(), "boom") {
				t.Errorf("stderr = %q, want underlying error %q", stderr.String(), "boom")
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty on error", stdout.String())
			}
		})
	}
}

func TestRenderStatusPausedRunning(t *testing.T) {
	cases := []struct {
		paused bool
		want   string
	}{
		{true, "Paused - nothing syncs until you run: synosync resume"},
		{false, "Up to date with host"},
	}
	for _, tc := range cases {
		var buf bytes.Buffer
		renderStatus(&buf, ipc.StatusReport{Server: "host:6690", Paused: tc.paused})
		if !strings.Contains(buf.String(), tc.want) {
			t.Errorf("paused=%v: rendered %q, want %q", tc.paused, buf.String(), tc.want)
		}
	}
}

// TestControlVerbsReportPairingOnAFreshInstall pins the first-run advice: with no
// state database at all, "start it with `synosync daemon`" would send the user to
// a command that answers "no config found; run `synosync pair` first". An absent
// state database is definitive - nothing has ever been stored - so the CLI names
// the pairing step directly.
func TestControlVerbsReportPairingOnAFreshInstall(t *testing.T) {
	for _, verb := range []string{"status", "sync", "pause", "resume", "conflicts"} {
		t.Run(verb, func(t *testing.T) {
			pointAtEmptyDir(t)
			var stdout, stderr bytes.Buffer
			if code := run([]string{verb}, &stdout, &stderr); code != 1 {
				t.Fatalf("exit = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), "not paired yet") {
				t.Errorf("stderr = %q, want the pairing step named", stderr.String())
			}
			if strings.Contains(stderr.String(), "synosync daemon`") {
				t.Errorf("stderr = %q, should not send a fresh install to the daemon", stderr.String())
			}
		})
	}
}
