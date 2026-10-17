package actions

// Tests for the download idle-progress watchdog (idle.go).
//
// The scenario these lock down: a peer that goes silent MID-BODY without closing
// the socket. Before the watchdog the decode blocked forever - the op goroutine
// never returned, so the engine's drainBatch never joined and the whole sync cycle
// froze with nothing classified as failed. Now the turn fails as the transient
// ErrTransferStalled inside a bounded budget, with the connection poisoned so the
// pool re-dials the worker and the op stays queued.
//
// The mirror requirement is just as important: a transfer that is merely SLOW must
// not be killed. TestDownloadSlowDripSurvivesIdleBudget drives a transfer whose
// total duration far exceeds the budget while every inter-byte gap stays under it.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/sproto"
)

// shortDownloadIdle shrinks the download idle budget for the duration of one test
// so a stall resolves in milliseconds rather than the production 2 min. It restores
// the previous value at cleanup; the tests using it are single-turn and never run
// in parallel, so the package-level var is not raced.
func shortDownloadIdle(t *testing.T, d time.Duration) {
	t.Helper()
	prev := downloadIdleTimeout
	downloadIdleTimeout = d
	t.Cleanup(func() { downloadIdleTimeout = prev })
}

// stallReader delivers head once and then blocks until release is closed, modelling
// a server whose storage layer wedges mid-body: the socket stays open and the kernel
// keeps ACKing, so nothing below the application layer ever notices.
type stallReader struct {
	head    []byte
	release <-chan struct{}
}

func (r *stallReader) Read(p []byte) (int, error) {
	if len(r.head) > 0 {
		n := copy(p, r.head)
		r.head = r.head[n:]
		return n, nil
	}
	<-r.release
	return 0, io.EOF
}

// dripReader delivers content in chunk-sized pieces separated by gap, modelling a
// legitimately slow transfer: no single gap reaches the idle budget, but the whole
// transfer takes far longer than one.
type dripReader struct {
	content []byte
	chunk   int
	gap     time.Duration
}

func (r *dripReader) Read(p []byte) (int, error) {
	if len(r.content) == 0 {
		return 0, io.EOF
	}
	time.Sleep(r.gap)
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.content) {
		n = len(r.content)
	}
	copy(p, r.content[:n])
	r.content = r.content[n:]
	return n, nil
}

// stalledContentClient wires a Client to a mock server that answers one action turn
// with a reply whose "data" binary claims claimLen bytes, delivers head, and then
// goes silent until the returned release channel is closed. The release cleanup is
// registered AFTER serveMock's, so it runs first (LIFO) and the server goroutine can
// always be joined.
func stalledContentClient(t *testing.T, head []byte, claimLen int64) *Client {
	t.Helper()
	release := make(chan struct{})
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("data", sproto.Binary{
				Length: claimLen,
				Reader: &stallReader{head: head, release: release},
			})
	})
	t.Cleanup(func() { close(release) })
	return c
}

func TestDownloadStalledMidBodyFailsWithinIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	shortDownloadIdle(t, idle)
	// A claim well past downloadInlineLimit takes the streaming path - the shape that
	// stalled (bytes teed to the caller as they arrive).
	c := stalledContentClient(t, bytes.Repeat([]byte("x"), 8192), 1<<20)

	var buf bytes.Buffer
	start := time.Now()
	_, err := c.Download(testContext(t), "f1", 1, 0, &buf)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("Download err = %v, want ErrTransferStalled", err)
	}
	if elapsed > 20*idle {
		t.Fatalf("Download returned after %s, want the stall detected within a few idle budgets (%s)", elapsed, idle)
	}
	if !c.Broken() {
		t.Error("Broken() = false after a stalled transfer, want the connection poisoned so the pool re-dials it")
	}
	// The poisoned connection refuses further turns rather than reading the abandoned
	// reply's tail as the next action's.
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); !errors.Is(err, ErrConnBroken) {
		t.Errorf("second Download err = %v, want ErrConnBroken", err)
	}
}

func TestResumeDownloadStalledMidBodyFailsWithinIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	shortDownloadIdle(t, idle)
	c := stalledContentClient(t, bytes.Repeat([]byte("y"), 8192), 1<<20)

	var buf bytes.Buffer
	start := time.Now()
	_, err := c.ResumeDownload(testContext(t), "tok", "f1", 1, 0, 0, &buf)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("ResumeDownload err = %v, want ErrTransferStalled", err)
	}
	if elapsed > 20*idle {
		t.Fatalf("ResumeDownload returned after %s, want the stall detected within a few idle budgets (%s)", elapsed, idle)
	}
	if !c.Broken() {
		t.Error("Broken() = false after a stalled chunk, want the connection poisoned")
	}
}

func TestDownloadStalledBeforeAnyBodyByteFails(t *testing.T) {
	// The watchdog covers the whole reply read, not just the body: a server that
	// frames the binary header and then delivers nothing at all is the same wedge.
	const idle = 80 * time.Millisecond
	shortDownloadIdle(t, idle)
	c := stalledContentClient(t, nil, 1<<20)

	var buf bytes.Buffer
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("Download err = %v, want ErrTransferStalled", err)
	}
}

func TestDownloadSlowDripSurvivesIdleBudget(t *testing.T) {
	// The bound is an IDLE budget, not a fixed deadline: a transfer that keeps
	// delivering bytes must complete however long it takes in total. Here the whole
	// transfer runs several budgets long while every gap stays well under one.
	const idle = 200 * time.Millisecond
	const gap = 25 * time.Millisecond
	const chunk = 1024
	shortDownloadIdle(t, idle)

	content := bytes.Repeat([]byte("synosync"), 2048) // 16 KiB: 16 chunks, ~400ms
	if int64(len(content)) <= downloadInlineLimit {
		t.Fatalf("test content %d not larger than the inline limit %d", len(content), downloadInlineLimit)
	}
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("f1")).
			Set("hash", sproto.String(md4.ContentHash(content))).
			Set("data", sproto.Binary{
				Length: int64(len(content)),
				Reader: &dripReader{content: append([]byte(nil), content...), chunk: chunk, gap: gap},
			})
	})

	var buf bytes.Buffer
	start := time.Now()
	if _, err := c.Download(testContext(t), "f1", 1, 0, &buf); err != nil {
		t.Fatalf("slow but progressing Download: %v, want nil (the idle budget re-arms on progress)", err)
	}
	if elapsed := time.Since(start); elapsed < idle {
		t.Fatalf("drip transfer finished in %s, shorter than the idle budget %s - the test does not exercise re-arming", elapsed, idle)
	}
	if !bytes.Equal(buf.Bytes(), content) {
		t.Errorf("drip content mismatch: got %d bytes, want %d", buf.Len(), len(content))
	}
	if c.Broken() {
		t.Error("Broken() = true after a successful slow transfer")
	}
}

func TestDownloadIdleGuardDoesNotMaskContextCancel(t *testing.T) {
	// The guard only ever pulls the socket deadline earlier, so a cancellation still
	// wins: the caller must see its own context error, not the stall sentinel (the
	// engine's isTerminal reads cancellation, and mislabelling it would change the
	// loop's exit).
	const idle = 10 * time.Second // far longer than the cancel below
	shortDownloadIdle(t, idle)
	c := stalledContentClient(t, bytes.Repeat([]byte("z"), 8192), 1<<20)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	defer cancel()

	var buf bytes.Buffer
	_, err := c.Download(ctx, "f1", 1, 0, &buf)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Download err = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrTransferStalled) {
		t.Error("a cancelled download was reported as a stall")
	}
}

func TestIdleGuardInertWithoutBudget(t *testing.T) {
	// A non-positive budget (or a nil conn) yields an inert guard: it never touches
	// the connection and stop() is still safe to call.
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	g := watchIdle(client, 0)
	r := g.wrap(bytes.NewReader([]byte("abc")))
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("read through inert guard: %v", err)
	}
	g.stop()
	if g.stalled() {
		t.Error("inert guard reported a stall")
	}

	nilGuard := watchIdle(nil, time.Second)
	nilGuard.stop()
	if nilGuard.stalled() {
		t.Error("nil-conn guard reported a stall")
	}
}
