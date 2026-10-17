package actions

// This file implements the Phase-6a event-stream actions: long_poll (subscribe to
// a set of views and block until any of them has a pending change) and pull_event
// (read a view's recorded changes from a sync cursor forward). Both are thin typed
// wrappers over Client.do, mirroring the catalog actions: build the request params,
// run the turn (which injects "action"/"session" and maps any errno to a typed
// error), then decode the action-specific reply fields tolerantly. A long_poll that
// times out server-side returns an empty (non-nil) slice, not an error.

import (
	"context"
	"fmt"
	"math"

	"github.com/mfilippov/synosync/internal/sproto"
)

// MinPollTimeout is the smallest long_poll timeout the server accepts. A shorter
// value is rejected with 28672 ("Invalid timeout value."), so LongPoll raises
// anything below it rather than letting the whole poll turn fail.
const MinPollTimeout = 300

// Subscription is one entry of a long_poll subscribe set. The server expects a
// MAP per view - not the bare view id an earlier revision sent, which it rejected
// with 28672 and which spun the inbound loop on every cycle.
//
// SyncID is the cursor the poll is relative to: the server blocks until the view
// has an event NEWER than it. Sending 0 asks "is there anything at all", so a view
// that has ever had an event would return immediately, every time.
type Subscription struct {
	// ViewID is the view to subscribe to.
	ViewID int64
	// NodeID is the view root's node id.
	NodeID int64
	// SyncID is the last sync id the client has consumed for this view.
	SyncID int64
	// Path is the prefix filter; empty means the view root ("/").
	Path string
}

// subscribeLockID is the lock_id every subscription carries. The server compares it with its own
// node-lock counter and reports the view as changed for as long as ours is behind, and 0 is behind
// on every server: measured on DSM 7.2 / Drive 4.0.3, a subscription with lock_id 0 returned at once
// on every poll while 1 and 2^40 held for the full timeout. This client takes no node locks and
// shows none, so it claims a counter no server reaches and is woken by sync_id alone.
const subscribeLockID = math.MaxInt32

// pobject renders the subscription in the shape the server expects.
func (s Subscription) pobject() *sproto.Map {
	path := s.Path
	if path == "" {
		path = "/"
	}
	return sproto.NewMap().
		Set("sync_id", sproto.Int(s.SyncID)).
		Set("path", sproto.String(path)).
		Set("view_id", sproto.Int(s.ViewID)).
		Set("node_id", sproto.Int(s.NodeID)).
		Set("lock_id", sproto.Int(subscribeLockID)).
		Set("option_change_lock_id", sproto.Int(0)).
		Set("recursive", sproto.Int(1))
}

// LongPoll subscribes to the given view ids and blocks until the server reports
// that one of them has pending changes or the server-side timeout (in seconds)
// elapses. It returns the view ids that have changes; a clean timeout yields an
// empty slice rather than an error. The caller's ctx should carry a deadline a bit
// beyond timeout so the long blocking read is not cut short before the server
// replies (see netctx.Watch).
//
// Emptiness audit: a bare {} reply - no errno, no "changes" -
// decodes as a clean timeout. That is deliberately accepted, unlike List's
// rejection: "nothing pending" is the poll's steady-state answer, the engine
// treats it as a quiet tick, and the worst a half-formed reply can cost is one
// extra poll cycle of latency - it is never consumed as proof about any node.
func (c *Client) LongPoll(ctx context.Context, subscribe []Subscription, timeout int) ([]int64, error) {
	views := make(sproto.Array, 0, len(subscribe))
	for _, s := range subscribe {
		views = append(views, s.pobject())
	}
	if timeout < MinPollTimeout {
		timeout = MinPollTimeout
	}
	params := sproto.NewMap().
		Set("subscribe", views).
		Set("timeout", sproto.Int(timeout))
	// The reply carries a profile_digest; a poll that does not send the latest one back is answered
	// at once with profile_changed (and the view flagged) every time, so the "long" poll returned in
	// ~40 ms and the engine spun on it - a full local scan every few seconds on an idle daemon.
	if c.profileDigest != "" {
		params.Set("profile_digest", sproto.String(c.profileDigest))
	}
	// The idle watchdog every other action turn arms (actionIdleTimeout) is
	// deliberately disarmed here: this reply is withheld server-side for the whole
	// poll timeout by design, so "no bytes for minutes" is the healthy steady state
	// rather than a wedged peer. The caller's ctx - pollOnce's timeout + slack - is
	// the bound that belongs to this turn.
	reply, err := c.doIdle(ctx, "long_poll", params, 0)
	if err != nil {
		return nil, err
	}
	if d, ok := reply.String("profile_digest"); ok && d != "" {
		c.profileDigest = d
	}
	raw, ok := reply.Get("changes")
	if !ok {
		// No "changes" key is a clean timeout: nothing pending.
		return []int64{}, nil
	}
	arr, ok := raw.(sproto.Array)
	if !ok {
		return nil, fmt.Errorf("actions: long_poll changes is %T, want array: %w", raw, ErrUnexpectedReply)
	}
	return decodeChanges(arr)
}

// PullEvent reads the events recorded for a view from sinceSyncID (exclusive)
// forward; pass 0 for a cold start. It returns the decoded events plus the next
// sync cursor the caller should resume from. The cursor is taken from the reply's
// "next_sync_id" when present; otherwise it falls back to the highest SyncID among
// the returned events, leaving it at sinceSyncID when there are no events.
//
// Emptiness audit: a bare {} reply - no errno, no
// "event_list", no "next_sync_id" - decodes as (nil events, sinceSyncID). That is
// deliberately accepted, unlike List's rejection: an empty page is never consumed
// as proof about any node - the incremental syncView page loop reads it as
// "backlog drained" and simply stops paginating, the durable cursor stays where it
// was (the clamp below already forbids a rewind), and the next poll cycle re-pulls
// from the same cursor. The worst a half-formed reply can cost is one cycle of
// latency, never a lost or fabricated event.
//
// The incremental poll's first page is the exception: that very first
// page IS consumed as proof about the whole catalog ("no events → server tree is
// empty → plan every local file as a fresh upload"), so it drives
// PullEventEvidenced instead and rejects an unevidenced first page - where
// "evidenced" additionally counts a present next_sync_id, since that cursor is
// trusted enough to advance from.
func (c *Client) PullEvent(ctx context.Context, viewID, nodeID, sinceSyncID int64) (events []Event, nextSyncID int64, err error) {
	events, nextSyncID, _, err = c.pullEvent(ctx, viewID, nodeID, sinceSyncID)
	return events, nextSyncID, err
}

// PullEventEvidenced is PullEvent that additionally reports whether the reply was
// positively evidenced: a reply carrying an "errno" (the server answered the
// request), an "event_list" key (a present, possibly empty page), or a
// "next_sync_id" (a cursor the request is trusted enough to advance from)
// is evidence the empty answer is real, whereas a bare {} -
// none of those keys - is not proof the page is empty. Only the cold-start reader
// needs the distinction: its first page is consumed as proof about the whole
// catalog, so an unevidenced empty first page must be retried rather than read as
// "server empty". The incremental syncView path stays on the
// tolerant PullEvent, which ignores the flag (a bare {} there is a benign quiet
// tick, cursor clamp-guarded).
func (c *Client) PullEventEvidenced(ctx context.Context, viewID, nodeID, sinceSyncID int64) (events []Event, nextSyncID int64, evidenced bool, err error) {
	return c.pullEvent(ctx, viewID, nodeID, sinceSyncID)
}

// pullEvent is the shared core behind PullEvent and PullEventEvidenced. evidenced
// is true when the reply carried an "errno", an "event_list" key, or a
// "next_sync_id" - see PullEventEvidenced.
func (c *Client) pullEvent(ctx context.Context, viewID, nodeID, sinceSyncID int64) (events []Event, nextSyncID int64, evidenced bool, err error) {
	// path must be EMPTY here, not "/": the server rejects "/" with 28672
	// ("Unsupport path format."). The per-attribute offsets are provisions for
	// fine-grained resumability and are always 0 in practice.
	zeroOffset := func() *sproto.Map { return sproto.NewMap().Set("offset", sproto.Int(0)) }
	params := sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("sync_id", sproto.Int(sinceSyncID)).
		Set("node_id", sproto.Int(nodeID)).
		Set("path", sproto.String("")).
		Set("recursive", sproto.Int(1)).
		Set("file", zeroOffset()).
		Set("mtime", zeroOffset()).
		Set("mac_attribute", zeroOffset()).
		Set("exec_bit", zeroOffset())
	reply, err := c.do(ctx, "pull_event", params)
	if err != nil {
		return nil, 0, false, err
	}
	// A present errno (do only returns a zero one; a non-zero code became an error
	// above) is positive evidence the server actually answered this request.
	if _, ok := reply.Get("errno"); ok {
		evidenced = true
	}
	if raw, ok := reply.Get("event_list"); ok {
		// A present event_list key - even an empty array - is positive evidence of
		// a real, empty page.
		evidenced = true
		arr, ok := raw.(sproto.Array)
		if !ok {
			return nil, 0, false, fmt.Errorf("actions: pull_event event_list is %T, want array: %w", raw, ErrUnexpectedReply)
		}
		events, err = decodeEvents(arr)
		if err != nil {
			return nil, 0, false, err
		}
	}
	if next, ok := reply.Int("next_sync_id"); ok {
		// A present next_sync_id is positive evidence the server answered this
		// request, just like errno/event_list: the same reply
		// is consumed as the authoritative durable cursor below, so a reply we
		// trust enough to advance the cursor from cannot simultaneously be "no
		// evidence the page is real". A DSM whose steady-state empty reply is
		// {next_sync_id: N} (no errno, no event_list - the wire shapes are
		// unconfirmed, DESIGN §13) would otherwise wedge cold start forever: its
		// unevidenced page would otherwise be re-read every cycle with no progress.
		evidenced = true
		// Clamp the reported cursor so it can never move backward: a server that
		// echoes a stale or out-of-order next_sync_id below sinceSyncID would
		// otherwise rewind the durable per-view cursor and re-pull events the
		// caller already processed. The cursor is monotonic by construction; pin
		// it to at least the value we polled from.
		nextSyncID = next
		if nextSyncID < sinceSyncID {
			nextSyncID = sinceSyncID
		}
	} else {
		nextSyncID = sinceSyncID
		for _, e := range events {
			if e.SyncID > nextSyncID {
				nextSyncID = e.SyncID
			}
		}
	}
	return events, nextSyncID, evidenced, nil
}

// ProfileDigest is the profile_digest the last long_poll reply carried, empty before the first.
// The event connection is re-dialled, and its Client rebuilt, so the caller carries the digest
// from one Client to the next with SetProfileDigest.
func (c *Client) ProfileDigest() string { return c.profileDigest }

// SetProfileDigest seeds the digest the next long_poll sends back (see ProfileDigest).
func (c *Client) SetProfileDigest(d string) { c.profileDigest = d }
