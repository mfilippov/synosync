package engine

// What the engine has actually moved, so a sync in flight can be reported rather than
// guessed at.
//
// Before this the client reported STATE - running, connected, N pending - and nothing about
// motion. On a first sync of a large view that is the difference between "slow" and "stuck",
// and the user cannot tell them apart: the pending count does not fall while a directory
// level is still being applied, and no byte count exists at all. Two of the three bottlenecks
// this engine had were found by measuring it from OUTSIDE, with du and /proc/PID/io, because
// it says nothing about itself.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/mfilippov/synosync/internal/store"
)

// progress accumulates applied work for the lifetime of one engine.
//
// The counters are deliberately monotonic and process-local: they measure what THIS run has
// moved, not what the view contains, so a restart starts them over. A cumulative figure that
// survived restarts would have to be persisted per view, and it would answer a different
// question than the one an operator watching a sync is asking.
type progress struct {
	// ops is the number of queued operations applied cleanly.
	ops atomic.Int64
	// bytes is content transferred in either direction, from the op's own size. It counts
	// downloads and uploads; a mkdir, rename or remove moves no content and adds nothing.
	bytes atomic.Int64
	// lastContactUnixNano is when the server last answered a turn or a long poll; zero
	// until the first reply. See noteServerContact for why the socket state cannot
	// answer this.
	lastContactUnixNano atomic.Int64
	// startedUnixNano fixes the window the rate is computed over. It is set on the first
	// applied op rather than at construction, so time spent connecting, walking or planning
	// does not dilute the transfer rate into meaninglessness.
	startedUnixNano atomic.Int64

	// listing* describe a cold-start walk in flight: which view, how many directories and
	// entries it has listed, and since when (zero when no walk runs). A first sync of a large
	// view spends its opening minutes here with nothing applied and nothing queued, so without
	// them status reads exactly like a client that is stuck.
	listingView      atomic.Uint64
	listingDirs      atomic.Int64
	listingEntries   atomic.Int64
	listingSinceNano atomic.Int64

	// lastAppliedNano is when the last op was applied; recent samples the totals for Recent.
	lastAppliedNano atomic.Int64
	recent          recentRing
}

// record accounts one cleanly applied op.
func (p *progress) record(transferred int64) {
	p.startedUnixNano.CompareAndSwap(0, time.Now().UnixNano())
	p.ops.Add(1)
	if transferred > 0 {
		p.bytes.Add(transferred)
	}
	now := time.Now()
	p.lastAppliedNano.Store(now.UnixNano())
	p.recent.note(now, p.ops.Load(), p.bytes.Load())
}

// snapshot reports the totals and the window they accumulated over. A zero window (nothing
// applied yet) is reported as such rather than as an instant, so a caller does not divide by
// it.
func (p *progress) snapshot() (ops, bytes int64, window time.Duration) {
	ops = p.ops.Load()
	bytes = p.bytes.Load()
	if started := p.startedUnixNano.Load(); started > 0 {
		window = time.Since(time.Unix(0, started))
	}
	return ops, bytes, window
}

// Progress reports what this engine run has applied: operations, bytes of content, and how
// long it has been moving them. The window starts at the first applied op, so a rate derived
// from it describes transfer rather than including the connect and planning phases.
func (e *Engine) Progress() (ops, bytes int64, window time.Duration) {
	return e.progress.snapshot()
}

// transferredBytes reports how much CONTENT an op moved, for the progress counters.
//
// Only a download and an upload carry content; a directory create, a rename and a remove are
// metadata turns whose LocalSize is either zero or describes something other than a transfer,
// and counting those would inflate the figure an operator reads as throughput.
func transferredBytes(op store.Op) int64 {
	switch op.Type {
	case store.OpDownload, store.OpUpload:
		return op.LocalSize
	default:
		return 0
	}
}

// noteServerContact stamps the moment the server last answered this client.
//
// It exists because `status` could report "connected" for an arbitrarily long time
// without that meaning anything had been heard from the server since boot. The
// connection state describes a socket; this describes an exchange. A half-open
// connection, a server that accepts and never replies, or an event connection parked in
// a long poll that no longer returns all read as "connected", and the operator has no
// way to tell them from a healthy idle sync - which is exactly the state a sync client
// spends most of its life in.
//
// The two stamping sites cover every exchange: the mandated worker-lease helper
// (Engine.withWorker), through which every action turn runs, and a long poll that
// returned a reply. Both stamp only on SUCCESS, so a failed turn leaves the previous
// time standing and the line ages, which is the signal.
func (p *progress) noteServerContact() {
	p.lastContactUnixNano.Store(time.Now().UnixNano())
}

// lastContact reports when the server last answered, or the zero time if it never has
// during this run. The counters are process-local for the reason given above the type:
// a time inherited from a previous run would answer a different question.
func (p *progress) lastContact() time.Time {
	if ns := p.lastContactUnixNano.Load(); ns > 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

// LastContact reports when this engine last received a reply from the server, or the
// zero time if it has not yet during this run.
func (e *Engine) LastContact() time.Time { return e.progress.lastContact() }

// startListing marks a cold-start walk of viewID as begun, with its counters at zero.
func (p *progress) startListing(viewID uint64) {
	p.listingView.Store(viewID)
	p.listingDirs.Store(0)
	p.listingEntries.Store(0)
	p.listingSinceNano.Store(time.Now().UnixNano())
}

// noteListed accounts one listed directory holding entries children.
func (p *progress) noteListed(entries int) {
	p.listingDirs.Add(1)
	p.listingEntries.Add(int64(entries))
}

// endListing marks the walk finished, however it ended.
func (p *progress) endListing() { p.listingSinceNano.Store(0) }

// Listing reports the cold-start walk in flight, if any: the view, the directories and
// entries listed so far, and how long it has run. ok is false when no walk is running.
func (e *Engine) Listing() (viewID uint64, dirs, entries int64, elapsed time.Duration, ok bool) {
	since := e.progress.listingSinceNano.Load()
	if since == 0 {
		return 0, 0, 0, 0, false
	}
	return e.progress.listingView.Load(), e.progress.listingDirs.Load(), e.progress.listingEntries.Load(),
		time.Since(time.Unix(0, since)), true
}

// The recent rate: the totals' lifetime average misleads as soon as the mix changes (on a real
// NAS the average still read 74 MiB/s twenty minutes after the big files gave way to small ones
// moving at 15). record keeps a small ring of (time, ops, bytes) samples, at most one per
// recentSampleEvery, and recent measures against the oldest within recentWindow.
const (
	recentWindow      = time.Minute
	recentSampleEvery = 5 * time.Second
	recentSamples     = int(recentWindow/recentSampleEvery) + 2
)

type progressSample struct {
	at         time.Time
	ops, bytes int64
}

type recentRing struct {
	mu      sync.Mutex
	samples [recentSamples]progressSample
	n, next int
}

func (r *recentRing) note(now time.Time, ops, bytes int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n > 0 {
		last := r.samples[(r.next-1+recentSamples)%recentSamples]
		if now.Sub(last.at) < recentSampleEvery {
			return
		}
	}
	r.samples[r.next] = progressSample{at: now, ops: ops, bytes: bytes}
	r.next = (r.next + 1) % recentSamples
	if r.n < recentSamples {
		r.n++
	}
}

// rate is the work per second since the oldest sample inside recentWindow; ok is false while
// there is under ten seconds of history to measure over (the first moments of a run).
func (r *recentRing) rate(now time.Time, ops, bytes int64) (opsPerSec, bytesPerSec float64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var base *progressSample
	for i := 0; i < r.n; i++ {
		s := &r.samples[(r.next-r.n+i+recentSamples)%recentSamples]
		if now.Sub(s.at) <= recentWindow {
			base = s
			break
		}
	}
	if base == nil {
		return 0, 0, r.n > 0 // history exists, none of it recent: nothing moved for a minute
	}
	dt := now.Sub(base.at).Seconds()
	if dt < 10 {
		return 0, 0, false
	}
	return float64(ops-base.ops) / dt, float64(bytes-base.bytes) / dt, true
}

// Recent reports the work per second over about the last minute, ok false until there is enough
// history, and when the last op was applied (zero before the first).
func (e *Engine) Recent() (opsPerSec, bytesPerSec float64, ok bool, lastApplied time.Time) {
	p := &e.progress
	if n := p.lastAppliedNano.Load(); n > 0 {
		lastApplied = time.Unix(0, n)
	}
	opsPerSec, bytesPerSec, ok = p.recent.rate(time.Now(), p.ops.Load(), p.bytes.Load())
	return opsPerSec, bytesPerSec, ok, lastApplied
}
