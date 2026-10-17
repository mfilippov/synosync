package engine

// This file keeps a worker free for the tree walk.
//
// The pool hands workers out first come, first served, and a transfer holds its worker for the
// whole body: on a NAS with large files all workers spend minutes in downloads while the walk's
// next `list` waits for one. Measured on a real 500k-node home view: the walk listed 400-470
// entries/s with no transfers running and 74-96 with them, near zero during multi-GB files —
// the first sync finished hours later than the link allowed.
//
// While a walk runs (walkServerSubtree: the cold start and every rescan), every lease except the
// walk's own goes through a gate of Workers-1 slots, so one worker is always left for `list`.
// The walk's leases (leasedLister) bypass the gate and may use any free worker. Outside a walk,
// and with a single worker, there is no gate at all. The gate never preempts a transfer already
// running: a walk that starts with every worker busy waits for the first to finish, and keeps a
// worker from then on.

import (
	"context"
	"sync"
)

// walkLane is the gate's state.
type walkLane struct {
	mu      sync.Mutex
	walking int
	gate    chan struct{} // Workers-1 slots, made when a walk begins
	open    chan struct{} // closed when the last walk ends, releasing waiters
}

// beginWalk opens the reserved lane for a walk.
func (e *Engine) beginWalk() {
	e.lane.mu.Lock()
	defer e.lane.mu.Unlock()
	e.lane.walking++
	if e.lane.walking == 1 {
		e.lane.gate = make(chan struct{}, max(1, e.limits.Workers-1))
		e.lane.open = make(chan struct{})
	}
}

// endWalk closes it; leases waiting at the gate go on ungated.
func (e *Engine) endWalk() {
	e.lane.mu.Lock()
	defer e.lane.mu.Unlock()
	e.lane.walking--
	if e.lane.walking == 0 {
		close(e.lane.open)
		e.lane.gate, e.lane.open = nil, nil
	}
}

// enterBulk takes a gate slot for an ordinary lease while a walk runs. release undoes it; err is
// set only when ctx ended first.
func (e *Engine) enterBulk(ctx context.Context) (release func(), err error) {
	e.lane.mu.Lock()
	gate, open := e.lane.gate, e.lane.open
	e.lane.mu.Unlock()
	if gate == nil || e.limits.Workers <= 1 {
		return func() {}, nil
	}
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-open:
		return func() {}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
