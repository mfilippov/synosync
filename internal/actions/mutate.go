package actions

// This file implements the Phase-3 structural mutation actions: renaming (or
// moving) a node and removing a single node. Both address nodes by
// human-readable, view-relative paths (DESIGN §2, "Addressing"), and both are thin
// single-turn wrappers over Client.do: they build the request params, run the
// turn (which injects "action"/"session" and maps any errno to a typed error),
// and report only success or that typed error - neither returns node metadata.

import (
	"context"
	"errors"
	"fmt"

	"github.com/mfilippov/synosync/internal/sproto"
)

// Cursor fields (max_id / sync_id / root_node_id / root_node_path) are deliberately
// NOT sent. The real Drive Client includes them, but it tracks the view cursor
// exactly; measured on a live DSM, a mutating request carrying ANY cursor value -
// current, stale, or zero - is refused with 40962/40961 and applies nothing, while
// the minimal form (view_id + the leading-slash path) applies reliably. This
// client's cursor can lag the server by construction, so sending one would make
// every delete and rename fail.
//
// Rename moves or renames the node at the view-relative path fromPath to toPath
// within the given view. A move and a rename are the same operation: the
// destination path carries both the new parent directory and the new name. The
// view_id scopes the paths to one view for the same reason QueryNode carries it
// - a view-relative path is ambiguous across views without it. The turn is
// bound by ctx so cancellation or a deadline interrupts the blocking I/O. A path
// error or a server-side timeout is reported as ErrPathError. The reply must
// carry an errno (doMutate): success here is what lets the caller record the
// move locally, so a half-formed reply without one is ErrUnexpectedReply rather
// than a phantom confirmation.
func (c *Client) Rename(ctx context.Context, viewID, rootNodeID int64, fromPath, toPath string) error {
	// rename REQUIRES a cursor, and a fresh one: measured on a live DSM it is refused
	// with 40961 when max_id/sync_id are absent, and the value must reflect the
	// server's current max - a caller-supplied cursor that has fallen behind draws
	// 40962 and applies nothing. So the cursor is fetched here, immediately before
	// the turn, rather than threaded in from the engine (whose view cursor lags by
	// construction). remove is the opposite and takes NO cursor at all.
	// The cursor is optimistic-concurrency state scoped to the OBJECT, not the view:
	// the server rejects a rename whose max_id/sync_id is below the source node's own
	// last-modification cursor with 40962 "Object has been modified". So the value
	// comes from the node itself (query_node's node.max_id) - one turn, and immune to
	// concurrent changes to OTHER nodes. The view-wide get_max_sync_id LAGS the log
	// and was measured failing under exactly that concurrency.
	//
	// A retry loop still guards the genuine race: another writer touching THIS node
	// between the read and the rename.
	var lastErr error
	for attempt := 0; attempt < renameCursorAttempts; attempt++ {
		syncID, err := c.nodeCursor(ctx, viewID, fromPath)
		if err != nil {
			return err
		}
		params := sproto.NewMap().
			Set("view_id", sproto.Int(viewID)).
			Set("src_path", sproto.String(serverPath(fromPath))).
			Set("dst_path", sproto.String(serverPath(toPath))).
			Set("max_id", sproto.Int(syncID)).
			Set("sync_id", sproto.Int(syncID)).
			Set("root_node_id", sproto.Int(rootNodeID)).
			Set("root_node_path", sproto.String("/"))
		lastErr = c.doMutate(ctx, "rename", params)
		if !isStaleCursor(lastErr) {
			return lastErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return lastErr
}

// nodeCursor returns the node's own last-modification cursor - query_node's
// node.max_id - which is what a mutating verb's max_id/sync_id is compared against
// ("Object has been modified"). Reading it from the node keeps the value tied to the
// object being changed, so unrelated concurrent commits cannot invalidate it.
func (c *Client) nodeCursor(ctx context.Context, viewID int64, path string) (int64, error) {
	reply, err := c.do(ctx, "query_node", sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("path", sproto.String(path)))
	if err != nil {
		return 0, err
	}
	raw, ok := reply.Get("node")
	if !ok {
		return 0, fmt.Errorf("actions: query_node reply missing node: %w", ErrUnexpectedReply)
	}
	nm, ok := raw.(*sproto.Map)
	if !ok {
		return 0, fmt.Errorf("actions: query_node node is %T, want map: %w", raw, ErrUnexpectedReply)
	}
	id, ok := nm.Int("max_id")
	if !ok {
		return 0, fmt.Errorf("actions: query_node node carries no max_id: %w", ErrUnexpectedReply)
	}
	return id, nil
}

// MaxSyncID returns the view's current server-side sync cursor. It is composed of
// two turns because neither alone is current: get_max_sync_id lags (measured 194
// while the event log had already reached 197), and pull_event from that point
// reports the true next_sync_id.
func (c *Client) MaxSyncID(ctx context.Context, viewID, rootNodeID int64) (int64, error) {
	reply, err := c.do(ctx, "get_max_sync_id", sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("path", sproto.String("/")).
		Set("root_node_id", sproto.Int(rootNodeID)).
		Set("root_node_path", sproto.String("/")))
	if err != nil {
		return 0, err
	}
	id, ok := reply.Int("max_sync_id")
	if !ok {
		return 0, fmt.Errorf("actions: get_max_sync_id reply missing max_sync_id: %w", ErrUnexpectedReply)
	}
	// Advance to the log's real tail: pull from the reported max and take the
	// next_sync_id the server answers with. Starting at id keeps the page tiny.
	_, next, perr := c.PullEvent(ctx, viewID, rootNodeID, id)
	switch {
	case perr == nil:
	case errors.Is(perr, ErrConnBroken), ctx.Err() != nil:
		// Two failures the fallback must NOT absorb. A poisoned connection means the turn left
		// the stream misaligned, so reporting a cursor as if the probe had answered hands the
		// caller a value produced by a connection it is about to have re-dialled anyway; and a
		// cancelled context is the caller's own shutdown, which every other turn propagates.
		// Swallowing either returned a "successful" lagging cursor and made a cold start that
		// was actually aborted look like one that merely found a quiet log.
		return 0, fmt.Errorf("actions: get_max_sync_id tail probe: %w", perr)
	default:
		// Anything else: the reported max is a valid, merely lagging answer, and the walk that
		// consumes it replays forward from there. Failing the turn over a probe that was only
		// an optimisation would be worse.
		return id, nil
	}
	if next > id {
		return next, nil
	}
	return id, nil
}

// serverPath renders a view-relative path in the leading-slash form the mutating
// verbs address ("" -> "/", "a/b" -> "/a/b"). The engine works in relative paths
// (viewRelative strips the slash at decode); the wire wants it back, and a bare
// relative path is rejected with 40961 without deleting anything.
func serverPath(rel string) string {
	if rel == "" || rel == "/" {
		return "/"
	}
	if rel[0] == '/' {
		return rel
	}
	return "/" + rel
}

// Remove deletes the single node at the view-relative path within the given
// view. It removes exactly one node per call; the caller is responsible for
// ordering children before their parents (DESIGN §2, "Structural actions"). The
// view_id scopes the path to one view (see Rename/QueryNode). The turn is bound
// by ctx. A path error (a missing or unreachable node) or a server-side timeout
// is reported as ErrPathError. Like Rename, the reply must carry an errno
// (doMutate) so a half-formed reply never confirms an unperformed deletion.
func (c *Client) Remove(ctx context.Context, viewID int64, path string) error {
	params := sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("path", sproto.String(serverPath(path)))
	return c.doMutate(ctx, "remove", params)
}

// renameCursorAttempts bounds how many times a rename re-reads the cursor after a
// stale-cursor rejection. The window is genuinely contended - this client's own
// engine commits on other connections while the cursor is being read - so a couple
// of retries is not enough in practice; each costs two cheap turns, and the bound
// still keeps a server that always answers 40962 from spinning the worker.
const renameCursorAttempts = 12

// isStaleCursor reports whether err is the server's optimistic-concurrency
// rejection (40962), which is retryable with a re-read cursor rather than a failure.
func isStaleCursor(err error) bool {
	if err == nil {
		return false
	}
	var se *ServerError
	return errors.As(err, &se) && se.Code == CodeStaleCursor
}
