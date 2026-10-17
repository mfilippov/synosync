package engine

// One way to read the pending queue for a single view.
//
// Ten call sites across the engine open the same three lines: scan the durable queue, drop
// everything belonging to another view, and wrap a scan failure. Each also has to remember
// that errStopScan is a control signal and not a failure - and getting THAT wrong is not
// cosmetic. Several of these scans exist to answer "does something already own this path",
// and their callers read an error as "the queue is unreadable, so refuse". An early stop
// reported as an error would make a successful lookup look like an unreadable queue; an
// unreadable queue reported as success would let a destructive apply proceed over state an
// owner was relying on. The rule belongs in one place.

import (
	"errors"
	"fmt"

	"github.com/mfilippov/synosync/internal/store"
)

// scanQueuedInView runs fn over every pending queue op belonging to viewID.
//
// Returning errStopScan from fn ends the scan early and is NOT an error: a caller that
// only needs to know whether a match exists stops at the first one instead of decoding the
// rest of the queue. Any other error from fn, and any store failure, comes back wrapped
// with what was being looked for - `what` names it in the caller's own words, so the
// message stays as specific as the hand-written ones it replaces.
//
// Two scans are deliberately CROSS-view - the stuck-op detail lookup and the whole-queue
// path index - and call store.ScanPending directly; both say so at their call site.
func (e *Engine) scanQueuedInView(viewID uint64, what string, fn func(store.QueuedOp) error) error {
	err := e.store.ScanPending(func(qo store.QueuedOp) error {
		if qo.ViewID != viewID {
			return nil
		}
		return fn(qo)
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return fmt.Errorf("engine: scan queued %s in view %d: %w", what, viewID, err)
	}
	return nil
}
