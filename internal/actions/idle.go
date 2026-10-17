package actions

// This file implements the idle-progress watchdog that bounds the streamed
// download replies (Download / ResumeDownload). Their reply length is
// server-controlled, so the engine deliberately excludes them from the fixed
// per-turn deadline (limits.op_turn_timeout_s): a legitimately slow multi-GiB
// transfer would be aborted on every drain and restart from scratch, so the
// timeout would wedge the very op it protects (internal/engine/transport.go).
//
// That left one hole: nothing bounded a transfer that makes NO progress at all.
// A storage layer that wedges mid-body (a stalled DSM, a SIGSTOPped server -
// the kernel keeps ACKing, so TCP never notices) leaves the codec blocked inside
// io.ReadFull forever. The op goroutine never returns, so the executor's
// drainBatch never joins, the whole sync cycle freezes and the stuck-op counter
// never moves: a healthy-looking daemon with a frozen queue, curable only by a
// restart.
//
// An IDLE deadline is not a fixed one: it bounds the gap between successive
// bytes, not the transfer. Any transfer that keeps delivering bytes - however
// slowly - re-arms it and runs to completion, while a genuinely silent socket
// fails inside a bounded budget. It is implemented as a watchdog rather than a
// per-read SetReadDeadline so it composes with netctx.Watch, which forces a PAST
// deadline on ctx cancellation: this guard only ever pulls the deadline earlier
// (never sets a future one), so it can never postpone a cancellation.

import (
	"io"
	"net"
	"sync/atomic"
	"time"
)

// downloadIdleTimeout bounds how long a streamed download reply may go with no
// read progress at all before the turn is failed as ErrTransferStalled. It is
// deliberately generous: a stall this long is a dead peer, not a slow one - a
// transfer moving even a single byte per budget keeps running. It is a var
// solely so tests can shorten it.
var downloadIdleTimeout = 2 * time.Minute

// idleGuard forces a past read deadline on a connection once no read progress
// has been reported for its budget, unblocking a decode that would otherwise
// wait on a silent peer forever. Progress is reported by the reader returned
// from wrap; the watchdog itself only ever tightens the deadline.
type idleGuard struct {
	conn net.Conn
	idle time.Duration
	// last is the UnixNano timestamp of the most recent read progress.
	last atomic.Int64
	// fired records that the watchdog expired the deadline, so the caller can
	// report the resulting I/O timeout as a stall rather than a generic error.
	fired  atomic.Bool
	done   chan struct{}
	exited chan struct{}
}

// watchIdle starts an idle watchdog over conn with the given budget. A
// non-positive budget (or a nil conn) yields an inert guard, so a caller can
// always wrap and always stop it. The returned guard's stop must be called once
// the turn completes; it joins the watchdog goroutine.
func watchIdle(conn net.Conn, idle time.Duration) *idleGuard {
	g := &idleGuard{
		conn:   conn,
		idle:   idle,
		done:   make(chan struct{}),
		exited: make(chan struct{}),
	}
	g.last.Store(time.Now().UnixNano())
	if conn == nil || idle <= 0 {
		close(g.exited)
		return g
	}
	go g.run()
	return g
}

// idleCheckDivisor sets how often the watchdog samples the progress timestamp
// relative to the budget. Sampling at a quarter of the budget bounds detection
// to at most 1.25× the budget while keeping the timer cheap (one tick per 30s
// on the production budget).
const idleCheckDivisor = 4

func (g *idleGuard) run() {
	defer close(g.exited)
	tick := g.idle / idleCheckDivisor
	if tick < time.Millisecond {
		tick = time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-g.done:
			return
		case now := <-t.C:
			if now.UnixNano()-g.last.Load() < int64(g.idle) {
				continue
			}
			// No byte arrived for a whole budget: expire any in-flight read. A
			// non-zero time in the past aborts it at once. Only the read side is
			// touched - this guard makes no claim about writes.
			g.fired.Store(true)
			_ = g.conn.SetReadDeadline(time.Unix(1, 0))
			return
		}
	}
}

// progress re-arms the budget. It is called from the read path for every read
// that delivered at least one byte.
func (g *idleGuard) progress() { g.last.Store(time.Now().UnixNano()) }

// stalled reports whether the watchdog expired the deadline (as opposed to the
// read failing for its own reasons, or the caller's context cancelling it).
func (g *idleGuard) stalled() bool { return g.fired.Load() }

// stop joins the watchdog goroutine. It deliberately does NOT clear the socket
// deadline: a guard that fired leaves the connection poisoned and unusable
// (the caller marks the Client broken so the pool re-dials it), and the caller's
// netctx.Watch owns clearing the deadline on the clean path.
func (g *idleGuard) stop() {
	select {
	case <-g.done:
	default:
		close(g.done)
	}
	<-g.exited
}

// wrap returns r instrumented to report read progress to the guard. It is passed
// to sproto.Conn.DecoderWith so every byte the codec pulls off the socket -
// header, metadata and streamed body alike - re-arms the budget.
func (g *idleGuard) wrap(r io.Reader) io.Reader { return &progressReader{r: r, g: g} }

// progressReader reports every non-empty read to its guard.
type progressReader struct {
	r io.Reader
	g *idleGuard
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.g.progress()
	}
	return n, err
}

// progressWriteChunk bounds how many bytes one Write on the underlying connection
// may carry before the guard is re-armed. A net.Conn Write returns only once the
// WHOLE slice has been handed to the kernel, so a single 4 MiB body write on a slow
// link reports progress exactly once - at the end. Splitting it re-arms the budget
// per chunk, which is what makes the guard an idle deadline rather than a fixed one
// on the write side.
//
// The granularity, stated honestly: the write side is idle-per-CHUNK, not
// idle-per-byte the way the read side is. Bytes the kernel accepts and the peer
// drains DURING a blocked Write are invisible to the guard, so one chunk that takes a
// whole budget to hand over fires the watchdog on a perfectly live connection. That
// leaves a floor rate - this chunk size over the turn's budget - below which a
// legitimately slow uplink is aborted mid-chunk and the op
// re-pushes from byte zero on every drain: the fixed-deadline degeneration this
// file's doctrine rejects, merely moved down to one chunk. At 64 KiB over the
// 5-minute action budget that floor was ~218 B/s, low but not unreachable on a
// congested or shaped link; 4 KiB puts it at
// ~13.7 B/s, below any TCP connection that is moving at all.
//
// Full per-byte re-arm is not reachable on stdlib TLS: the body is written through
// tls.Conn, which reports nothing until it has framed, sealed and flushed a whole
// record. 4 KiB is a quarter of the 16 KiB maximum record, so the cost of the finer
// granularity is ~4× more records - roughly half a percent of extra header/tag bytes
// and one more syscall per 4 KiB - paid to keep the guarantee honest rather than the
// syscall count low.
const progressWriteChunk = 4 << 10

// wrapWriter returns w instrumented to report WRITE progress to the guard, splitting
// oversized writes at progressWriteChunk so a slow-but-moving transfer re-arms the
// budget mid-frame.
//
// It exists for the two-frame upload turns (Upload / ResumeUpload), which hand the
// body frame to a concurrent writer and THEN wait for the reply. By protocol no reply
// byte can arrive until the server has drained that body, so a read-only guard would
// see zero progress for the whole body transfer and degenerate into a fixed deadline
// on it - aborting a legitimately slow upload on every drain and wedging the very op
// it protects, the exact failure mode the doctrine at the top of this file rejects.
// Reporting the body write as progress bounds only genuine silence: a peer that stops
// draining stalls the write too, so nothing re-arms and the guard still fires.
func (g *idleGuard) wrapWriter(w io.Writer) io.Writer { return &progressWriter{w: w, g: g} }

// progressWriter reports every non-empty write to its guard, capping each underlying
// Write at progressWriteChunk so one bulk write cannot span the whole budget.
type progressWriter struct {
	w io.Writer
	g *idleGuard
}

func (p *progressWriter) Write(b []byte) (int, error) {
	total := 0
	for total < len(b) {
		chunk := b[total:]
		if len(chunk) > progressWriteChunk {
			chunk = chunk[:progressWriteChunk]
		}
		n, err := p.w.Write(chunk)
		total += n
		if n > 0 {
			p.g.progress()
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			// A writer that accepts nothing without reporting an error would spin here;
			// treat it as the short-write violation it is rather than loop forever.
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
