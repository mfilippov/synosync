package actions

// Tests for the idle-progress watchdog on the TWO-FRAME upload turns (Upload,
// ResumeUpload).
//
// These turns are the one shape where the guard cannot be read-only. The body frame
// is handed to a concurrent writer and the reply is awaited immediately, but by
// protocol no reply byte can arrive until the server has drained that body - so a
// guard fed only by read progress sees the ENTIRE body transfer as silence and
// degenerates into a fixed deadline on it. On a link slower than
// upload_resume_threshold / actionIdleTimeout every attempt would abort as
// ErrTransferStalled, the connection would be re-dialled and the op re-pushed from
// byte zero forever: the fixed-deadline wedge the idle doctrine exists to avoid
// (idle.go). The body write therefore reports its own progress through the same
// guard (idleGuard.wrapWriter).
//
// The mirror requirement is that this must not blunt the guard: a peer that stops
// draining stalls the write too, so nothing re-arms the budget and the turn still
// fails inside it.

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/sproto"
)

// pacedReader rate-limits a reader to chunk bytes per pause: it caps every Read at
// chunk bytes and sleeps once per chunk of payload actually delivered. The pacing is
// per BYTE, not per Read, so the frame's small prefix reads cost nothing and only the
// bulk payload is slowed - a bandwidth limit rather than a per-syscall one. Over an
// unbuffered net.Pipe that is exactly what a slow link looks like from the client's
// write side: each Write returns only as fast as the peer reads.
type pacedReader struct {
	r     io.Reader
	chunk int
	pause time.Duration
	acc   int
}

func (p *pacedReader) Read(b []byte) (int, error) {
	if len(b) > p.chunk {
		b = b[:p.chunk]
	}
	n, err := p.r.Read(b)
	p.acc += n
	for p.acc >= p.chunk {
		p.acc -= p.chunk
		time.Sleep(p.pause)
	}
	return n, err
}

// slowDrainServer reads the CmdAction header and the metadata frame at full speed,
// drains the body frame through a pacedReader, and then answers with reply.
func slowDrainServer(chunk int, pause time.Duration, reply func() sproto.Value) func(t *testing.T, server net.Conn) {
	return func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		slow := &pacedReader{r: server, chunk: chunk, pause: pause}
		if _, err := sproto.NewDecoder(slow).Decode(); err != nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(reply())
	}
}

// wedgedBodyServer reads the metadata frame and then neither drains the body nor
// replies, holding the connection open. The release cleanup must be registered AFTER
// serveMock's so it runs first (LIFO) and the server goroutine can be joined.
func wedgedBodyServer(t *testing.T) *sproto.Conn {
	t.Helper()
	release := make(chan struct{})
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		<-release
	})
	t.Cleanup(func() { close(release) })
	return conn
}

// slowBody is sized so the paced drain below spans several idle budgets while no
// single write chunk (progressWriteChunk, 4 KiB) ever takes one.
func slowBody() []byte {
	b := make([]byte, 768<<10)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestSlowlyDrainedUploadBodyIsNotStalled(t *testing.T) {
	const idle = 200 * time.Millisecond
	shortActionIdle(t, idle)
	content := slowBody()

	// 16 KiB per 12 ms: the whole body takes ~576 ms - well past the budget - while a
	// progressWriteChunk completes every ~48 ms, so the guard is re-armed throughout.
	conn := serveMock(t, slowDrainServer(16<<10, 12*time.Millisecond, func() sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("newf")).
			Set("hash", sproto.String(md4.ContentHash(content)))
	}))
	c := newClient(conn, "sess")

	start := time.Now()
	node, err := c.Upload(testContext(t), "p", "big.bin", 0, 1, content)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Upload err = %v, want nil: a body the server drains slowly must re-arm the idle budget, not be aborted by it", err)
	}
	if elapsed <= idle {
		t.Fatalf("Upload finished in %s, want a transfer outlasting the whole idle budget (%s) so the re-arm is what carried it", elapsed, idle)
	}
	if node.FileID != "newf" {
		t.Errorf("node.FileID = %q, want newf", node.FileID)
	}
	if c.Broken() {
		t.Error("Broken() = true after a slow but successful upload, want the connection reusable")
	}
}

func TestSlowlyDrainedResumeUploadChunkIsNotStalled(t *testing.T) {
	const idle = 200 * time.Millisecond
	shortActionIdle(t, idle)
	chunk := slowBody()

	conn := serveMock(t, slowDrainServer(16<<10, 12*time.Millisecond, func() sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("file_id", sproto.String("slow"))
	}))
	c := newClient(conn, "sess")

	start := time.Now()
	node, err := c.ResumeUpload(testContext(t), "tok",
		ResumeUploadSpec{ParentID: "p", Name: "slow.bin", RootNodeID: 1, Size: int64(len(chunk)), Hash: "h"},
		bytes.NewReader(chunk))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("ResumeUpload err = %v, want nil: a body the server drains slowly must re-arm the idle budget", err)
	}
	if elapsed <= idle {
		t.Fatalf("ResumeUpload finished in %s, want a transfer outlasting the whole idle budget (%s)", elapsed, idle)
	}
	if node.FileID != "slow" {
		t.Errorf("node = %+v, want the created node", node)
	}
	if c.Broken() {
		t.Error("Broken() = true after a slow but successful resume upload")
	}
}

// TestWedgedUploadBodyStillStallsWithinTheIdleBudget is the mirror: reporting write
// progress must not disarm the guard. A peer that stops draining the body reports no
// write progress either - the write blocks - so nothing re-arms the budget and the
// turn still fails as the transient ErrTransferStalled with the connection poisoned.
func TestWedgedUploadBodyStillStallsWithinTheIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	shortActionIdle(t, idle)
	c := newClient(wedgedBodyServer(t), "sess")

	start := time.Now()
	_, err := c.Upload(testContext(t), "p", "big.bin", 0, 1, make([]byte, 256<<10))
	elapsed := time.Since(start)

	if !errors.Is(err, ErrTransferStalled) {
		t.Fatalf("Upload err = %v, want ErrTransferStalled", err)
	}
	if elapsed > 20*idle {
		t.Fatalf("Upload returned after %s, want the stall detected within a few idle budgets (%s)", elapsed, idle)
	}
	if !c.Broken() {
		t.Error("Broken() = false after a stalled upload, want the connection poisoned so the pool re-dials it")
	}
}
