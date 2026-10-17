package actions

// This file implements the Phase-3 read/metadata actions: enumerating the
// account's views, reading a view's sync cursor, inspecting a single catalog node,
// and listing a directory. Each is a thin typed wrapper over Client.do: it builds
// the request params, runs the turn (which injects "action"/"session" and maps any
// errno to a typed error), then decodes the action-specific reply fields. Missing
// optional fields are tolerated; a reply of the wrong shape is ErrUnexpectedReply.
// Exception: a read verb whose EMPTINESS is itself the evidence its callers
// consume must not decode a contentless reply as that evidence - List requires
// errno-or-node_list and QueryUserInfo requires errno-or-view_list
// (QueryNode already fails on an absent "file", and LongPoll/PullEvent
// emptiness is audited harmless in events.go). Beyond that floor, such a verb
// must also let the caller tell an enumerated-empty reply from an unenumerated
// one: List returns an explicit listed flag and QueryUserInfo a nil-vs-empty
// slice.

import (
	"context"
	"fmt"

	"github.com/mfilippov/synosync/internal/sproto"
)

// QueryUserInfo enumerates the views (shared folders) the account can sync. The
// reply carries them under "view_list": a PRESENT list decodes to a non-nil slice
// (empty for a present-but-empty list - an account with every view revoked), while
// an absent list yields a nil slice rather than an error - but only when the reply
// carries at least one of "errno" / "view_list". A bare {} with NEITHER is rejected
// as ErrUnexpectedReply (mirroring List's evidence requirement): the engine consumes the enumeration's emptiness as positive
// evidence - a stored view missing from it is marked absent and its sync
// suppressed (reconcileAbsentViews) - so a contentless half-formed reply must not
// read as an authoritative empty enumeration, which would mark EVERY view absent
// and silence the whole client. The whole reply was read, so the connection stays
// byte-aligned and reusable; an errno:0 reply without "view_list" keeps returning
// nil, nil (and the engine treats that nil as no evidence either way).
func (c *Client) QueryUserInfo(ctx context.Context) ([]View, error) {
	// get_view=1 is what makes the server include view_list at all. Without it a
	// live DSM answers with the user's profile and no view_list and no errno -
	// which this function correctly rejects as contentless, so the engine's view
	// refresh failed on every pass and the client never learned a single view.
	params := sproto.NewMap().Set("get_view", sproto.Int(1))
	reply, err := c.do(ctx, "query_user_info", params)
	if err != nil {
		return nil, err
	}
	raw, ok := reply.Get("view_list")
	if !ok {
		if _, present, _ := errno(reply); !present {
			return nil, fmt.Errorf("actions: query_user_info reply carries neither a result verdict nor view_list: %w", ErrUnexpectedReply)
		}
		return nil, nil
	}
	arr, ok := raw.(sproto.Array)
	if !ok {
		return nil, fmt.Errorf("actions: query_user_info view_list is %T, want array: %w", raw, ErrUnexpectedReply)
	}
	views := make([]View, 0, len(arr))
	for _, el := range arr {
		m, ok := el.(*sproto.Map)
		if !ok {
			return nil, fmt.Errorf("actions: query_user_info view is %T, want map: %w", el, ErrUnexpectedReply)
		}
		views = append(views, decodeView(m))
	}
	return views, nil
}

// QueryNode inspects a single catalog node addressed by a human-readable,
// view-relative path within the given view. The reply carries the node under
// "file". The view_id scopes the path to one view: a view-relative path (notably
// the empty root path) is ambiguous across views without it, so the same path -
// e.g. the root - would otherwise resolve identically for every view.
func (c *Client) QueryNode(ctx context.Context, viewID int64, path string) (Node, error) {
	params := sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("path", sproto.String(path))
	reply, err := c.do(ctx, "query_node", params)
	if err != nil {
		return Node{}, err
	}
	// The node comes back under "node"; "file" is accepted as a fallback for the
	// in-memory fakes that were written against the earlier assumption.
	raw, ok := reply.Get("node")
	if !ok {
		raw, ok = reply.Get("file")
	}
	if !ok {
		return Node{}, fmt.Errorf("actions: query_node reply missing node: %w", ErrUnexpectedReply)
	}
	m, ok := raw.(*sproto.Map)
	if !ok {
		return Node{}, fmt.Errorf("actions: query_node node is %T, want map: %w", raw, ErrUnexpectedReply)
	}
	return decodeNode(m), nil
}

// List enumerates the immediate children of a directory addressed by a
// view-relative path within the given view. The view_id scopes the path to one
// view (see QueryNode) - a view-relative path is ambiguous across views without
// it. The children ride under "node_list" - the key a live DSM 7.x / Drive
// Server 4.0.3 actually sends; "file_list" is read only as a fallback (see the
// decode below).
//
// The second result, listed, is the ENUMERATION EVIDENCE: it is true only when
// the reply actually carried a children array, so a caller can tell a reply
// that enumerated an empty directory (nodes empty, listed true - positive proof
// the directory has no children) from one that enumerated nothing at all (nodes
// nil, listed false). Unlike the other read actions, whose callers decode
// action-specific fields as their positive evidence, List's EMPTINESS is itself
// the evidence its callers consume - the engine's parent-listing absence probes
// (confirmAbsentByParentList / resolveServerNodeAbsence, nodeGoneByID and the
// ancestorConfirmsAbsent walk) take an empty listing as positive proof a node is
// gone. An errno-only reply iterating as a zero-length list would therefore ack
// an unperformed remove - a silently lost local delete - or spuriously
// dead-letter live ops, so those callers require listed and
// treat an errno-only reply as inconclusive, exactly as reconcileAbsentViews
// guards QueryUserInfo's nil view slice.
//
// An errno-only reply is therefore NOT an error here (the turn itself succeeded
// and the whole reply was read, so the connection stays byte-aligned and
// reusable) - it is (nil, false, nil). A bare {} carrying NEITHER a result
// verdict NOR a children array is still rejected as ErrUnexpectedReply (the
// read-verb mirror of doMutate's errno requirement): a reply with no evidence of
// any kind is a half-formed turn, not a sparse one.
func (c *Client) List(ctx context.Context, viewID int64, path string) (nodes []Node, listed bool, err error) {
	params := sproto.NewMap().
		Set("view_id", sproto.Int(viewID)).
		Set("path", sproto.String(path)).
		Set("list_dir_only", sproto.Int(0)).
		Set("include_node_locking", sproto.Int(0)).
		Set("merge_local", sproto.Int(0))
	reply, err := c.do(ctx, "list", params)
	if err != nil {
		return nil, false, err
	}
	// The children ride under "node_list"; "file_list" is accepted as a fallback
	// for the in-memory fakes written against the earlier assumption.
	raw, ok := reply.Get("node_list")
	if !ok {
		raw, ok = reply.Get("file_list")
	}
	if !ok {
		if _, present, _ := errno(reply); !present {
			return nil, false, fmt.Errorf("actions: list reply carries neither a result verdict nor node_list: %w", ErrUnexpectedReply)
		}
		return nil, false, nil
	}
	arr, ok := raw.(sproto.Array)
	if !ok {
		return nil, false, fmt.Errorf("actions: list node_list is %T, want array: %w", raw, ErrUnexpectedReply)
	}
	nodes = make([]Node, 0, len(arr))
	for _, el := range arr {
		m, ok := el.(*sproto.Map)
		if !ok {
			return nil, false, fmt.Errorf("actions: list entry is %T, want map: %w", el, ErrUnexpectedReply)
		}
		nodes = append(nodes, decodeNode(m))
	}
	return nodes, true, nil
}
