package engine

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"

	"github.com/mfilippov/synosync/internal/actions"
)

// Ops that failed a drain but have not yet reached stuckThreshold.
//
// A failed transient apply stays queued and is retried at the next leading drain, which runs
// only after the current long poll returns - up to EventPollTimeout (~5 min) later. The Stuck
// line needs stuckThreshold such failures, so for the first several drains (tens of minutes on
// an idle sync) `status` said only "N items to go", exactly as for work that is moving, and an
// operator watching it could not tell a pending retry from a hang (issue #6). The band below the
// threshold is reported separately as Retrying, with the last failure's reason.

// reasonBlocked is the reason recorded for an op skipped behind a predecessor on an overlapping
// path that did not complete this drain - failed, paused by its retry pacing, or replaced. It has
// no failure of its own to name, and the predecessor is not necessarily failing.
const reasonBlocked = "waiting behind another operation on the same path"

// failureReason words the class of a failed apply for `status`. It is deliberately coarse:
// the full error chain is in the log, and the point here is the one fact an operator acts on -
// is the network the problem, the server, or this item. An unrecognised error yields "", which
// the CLI renders as a pointer to the log.
//
// The network class is matched on CONCRETE network types only, never on the net.Error
// interface: syscall.Errno and the context/os deadline sentinels all satisfy net.Error, so that
// match named a local EACCES or a Windows sharing violation "the connection to the server was
// lost" - sending the operator to the network for a file problem. A local *fs.PathError or
// *os.LinkError is therefore classified first; net.OpError carries no PathError, so a socket error cannot land
// there.
func failureReason(err error) string {
	var dnsErr *net.DNSError
	var opErr *net.OpError
	var pathErr *fs.PathError
	var linkErr *os.LinkError // a rename's error, which carries no PathError
	switch {
	case err == nil:
		return ""
	case isDiskFull(err):
		return "the local disk is full"
	case errors.Is(err, errTurnTimeout):
		return "the server took too long to answer"
	case errors.Is(err, actions.ErrTransferStalled):
		return "the transfer stalled"
	case errors.Is(err, actions.ErrNodeBusy):
		return "the server is still settling this item"
	case errors.Is(err, errParentUnknown):
		return "its parent folder is not on the server yet"
	case errors.As(err, &pathErr), errors.As(err, &linkErr):
		return "a local file or folder could not be read or written"
	case errors.As(err, &dnsErr):
		return "the server's name could not be resolved"
	case errors.Is(err, actions.ErrConnBroken), errors.As(err, &opErr), errors.Is(err, net.ErrClosed):
		// Not a bare io.EOF: a large upload whose local file shrank mid-stream ends in one
		// too (actions/resume.go), and a real transport failure already carries one of these.
		return "the connection to the server was lost"
	case errors.Is(err, actions.ErrUnexpectedReply):
		return "the server sent an unexpected reply"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return "the operation timed out"
	default:
		return ""
	}
}

// setOpLastReason records the reason for an op's latest failed drain. Callers hold stuckMu.
func (e *Engine) setOpLastReason(seq uint64, reason string) {
	if e.opLastReason == nil {
		e.opLastReason = make(map[uint64]string)
	}
	e.opLastReason[seq] = reason
}

// RetryingOps reports how many queued ops failed at least one drain but fewer than
// stuckThreshold: work that is waiting for its next attempt rather than moving. Ops at or past
// the threshold are counted by StuckOps instead, so the two never overlap. Safe to call
// concurrently with Run.
func (e *Engine) RetryingOps() int {
	e.stuckMu.Lock()
	defer e.stuckMu.Unlock()
	n := 0
	for _, failures := range e.opFailures {
		if failures > 0 && failures < stuckThreshold {
			n++
		}
	}
	return n
}

// RetryingDetails lists the ops behind the RetryingOps count, worst first, capped at limit.
// Safe to call concurrently with Run, like StuckDetails.
func (e *Engine) RetryingDetails(limit int) ([]StuckOp, error) {
	return e.failingDetails(1, stuckThreshold, limit)
}
