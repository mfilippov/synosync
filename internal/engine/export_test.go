package engine

import (
	"fmt"

	"github.com/mfilippov/synosync/internal/normpath"
	"github.com/mfilippov/synosync/internal/store"
)

// This file holds test-only Engine helpers. Living in an _test.go file keeps them
// out of the shipped public API (they compile only under `go test`) while staying
// reachable from the engine package's own tests. Production installs the watcher's
// wake channel through SetWatcher (which wires the factory's channel into e.wake),
// never through a bare SetWake, so the direct setter stayed test-only.

// loadScanHints builds the cheap-scan hint map keyed by normpath.Key(path) from the view's
// stored baselines, the same way the production cheap-scan path does (the hints != nil branch
// of collectBaselines). It is test-only: after the single-pass refactor, production folds
// hint-building into collectBaselines' single ScanBaselinesForView stream, so this helper exists
// only to let the scanner unit tests build a hint table in isolation. Only regular files with a
// recorded content hash are included; directories (empty hash) and never-hashed baselines carry
// no hint and fall through to a full hash in scanView.
func (e *Engine) loadScanHints(viewID uint64) (map[string]scanHint, error) {
	hints := make(map[string]scanHint)
	_, err := e.scanBaselines(viewID, func(pb store.PathBaseline) error {
		if pb.FileType == "dir" || pb.LocalHash == "" {
			return nil
		}
		hints[normpath.Key(pb.Path)] = scanHint{
			size:  pb.LocalSize,
			mtime: pb.LocalMTime,
			hash:  pb.LocalHash,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("engine: load scan hints for view %d: %w", viewID, err)
	}
	return hints, nil
}

// SetWake installs the watcher's debounced wake channel (Phase 7) directly, the seam
// engine tests use to feed a fake wake without standing up fsnotify. While the poll
// loop is blocked in the long-poll, a receive on wake cancels that poll, re-dials the
// (now byte-misaligned) event connection, and resets the scan throttle so the next
// cycle's localScan runs immediately - turning a local change into a prompt push
// rather than waiting up to a full poll cycle. The channel should be buffered and
// coalescing so a wake fired while the loop is not blocked is not lost (the next poll
// consumes it). A nil channel preserves today's behaviour. SetWake must be called
// before Run and not concurrently with it; a wake installed this way takes precedence
// over a watcher factory (SetWatcher), which is then not invoked.
func (e *Engine) SetWake(wake <-chan struct{}) {
	e.wake = wake
}
