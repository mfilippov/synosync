//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/transport"
)

// TestIntegration_UploadBaseVersionProbe asks whether this server offers ANY
// optimistic-concurrency check on `upload` - a way for a client to say "apply this
// only if the node is still the version I last agreed with".
//
// The question follows directly from TestIntegration_UploadConflictSemantics: the
// server's compare_mtime policy accepts a newer-mtime overwrite unconditionally, so a
// client that pushes an edit it made against a stale base destroys the other client's
// version. `uploadParams` (actions/upload.go) already sends `max_id` and `sync_id`,
// both hardcoded to 0. On `rename` the SAME `max_id` field is the object's own version
// and a stale value is refused with 40962 ("Object has been modified"), which is
// exactly the check upload needs. Whether upload honours it is unmeasured - and it
// decides which fix is available: a race-free version precondition (send the baseline's
// remembered max_id) or a racy pre-flight probe (query the node, compare hashes).
//
// The typed client hardcodes those fields, so the probe builds the two-frame upload
// turn by hand over its own session. Nothing is asserted about the answer; it is a
// measurement, and both outcomes are recorded in docs/INTEGRATION.md.
func TestIntegration_UploadBaseVersionProbe(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	parent := sc.remoteMkdirAll(t, "baseversion")
	const name = "z.txt"
	rel := sc.viewRel("baseversion/" + name)

	base := time.Now().Unix()
	v1 := []byte("base-version probe: v1, the version the losing client agreed with\n")
	v2 := []byte("base-version probe: v2, committed by the OTHER client\n")
	v3 := []byte("base-version probe: v3, the losing client's push carrying a STALE max_id\n")
	v4 := []byte("base-version probe: v4, the same push carrying a FRESH max_id\n")

	ctx, cancel := actionCtx()
	if _, err := sc.client.Upload(ctx, parent.FileID, name, base, sc.view.RootNodeID, v1); err != nil {
		cancel()
		t.Fatalf("integration: upload v1: %v", err)
	}
	cancel()

	rawCtx, rawCancel := context.WithTimeout(context.Background(), 3*netTimeout)
	defer rawCancel()
	sess, err := sc.connector(rawCtx)
	if err != nil {
		t.Fatalf("integration: dial a session for the raw turns: %v", err)
	}
	defer func() { _ = sess.Close() }()

	staleMaxID := rawNodeMaxID(t, sc, sess, rel)
	t.Logf("agreed version: node max_id=%d", staleMaxID)

	// The other client commits its own version. The node's max_id must advance, or the
	// field carries no version at all and the probe below cannot mean anything.
	ctx, cancel = actionCtx()
	if _, err := sc.client.Upload(ctx, parent.FileID, name, base+600, sc.view.RootNodeID, v2); err != nil {
		cancel()
		t.Fatalf("integration: upload v2: %v", err)
	}
	cancel()
	freshMaxID := rawNodeMaxID(t, sc, sess, rel)
	t.Logf("after the peer's commit: node max_id=%d (was %d)", freshMaxID, staleMaxID)
	if freshMaxID == staleMaxID {
		t.Logf("VERDICT: max_id does not advance on a content change, so it cannot express a base version for upload")
	}

	// The losing push, carrying the version it last agreed with and a NEWER mtime -
	// the request the engine would send if it passed the baseline's remembered version.
	reply := rawUpload(t, sc, sess, parent.FileID, name, base+1200, staleMaxID, staleMaxID, v3)
	staleCode, staleHas := replyErrno(reply)
	t.Logf("STALE max_id=%d push: errno=%d present=%v", staleMaxID, staleCode, staleHas)
	for _, line := range dumpMap(reply, "    ") {
		t.Log(line)
	}

	landed, ok := serverQuery(t, sc.client, sc.view.ViewID, rel)
	if !ok {
		t.Fatalf("integration: the node vanished mid-probe")
	}
	switch {
	case landed.Hash == md4.ContentHash(v3):
		t.Logf("VERDICT: the STALE-version push LANDED - upload has no base-version precondition on this server, " +
			"so the only client-side fix is a pre-flight probe (query the node and compare its hash " +
			"to the baseline) before pushing a modify")
	case landed.Hash == md4.ContentHash(v2):
		t.Logf("VERDICT: the STALE-version push was REFUSED and the peer's version survived - upload DOES honour " +
			"max_id as a base version, so sending the baseline's remembered max_id is a race-free fix " +
			"for the concurrent-edit data loss")
	default:
		t.Logf("VERDICT: unexpected post-probe content (hash %s)", landed.Hash)
	}

	// Control: the same raw turn with a FRESH version must land, or the probe above
	// proves nothing (it could have been refused for the raw shape, not the version).
	fresh := rawNodeMaxID(t, sc, sess, rel)
	reply = rawUpload(t, sc, sess, parent.FileID, name, base+1800, fresh, fresh, v4)
	code, has := replyErrno(reply)
	t.Logf("CONTROL fresh max_id=%d push: errno=%d present=%v", fresh, code, has)
	after, ok := serverQuery(t, sc.client, sc.view.ViewID, rel)
	if !ok {
		t.Fatalf("integration: the node vanished after the control push")
	}
	if after.Hash != md4.ContentHash(v4) {
		t.Logf("CONTROL DID NOT LAND (hash %s): the raw two-frame shape itself is not accepted with a non-zero "+
			"max_id, so the stale probe above is inconclusive", after.Hash)
	} else {
		t.Logf("CONTROL landed: the raw shape is accepted, so the stale probe's outcome is about the VERSION")
	}
}

// rawNodeMaxID reads a node's own version counter (node.max_id) - the value `rename`
// sends as its cursor - over a raw query_node turn, since actions.Node does not carry
// it.
func rawNodeMaxID(t *testing.T, sc *scenario, sess *transport.Session, viewRelPath string) int64 {
	t.Helper()
	req := sc.rawRequest("query_node").
		Set("view_id", sproto.Int(sc.view.ViewID)).
		Set("path", sproto.String(viewRelPath))
	reply := rawTurn(t, sess, req, nil)
	node, ok := reply.Get("node")
	if !ok {
		t.Fatalf("integration: query_node reply carries no node: %v", dumpMap(reply, ""))
	}
	nm, ok := node.(*sproto.Map)
	if !ok {
		t.Fatalf("integration: query_node node is %T, want a map", node)
	}
	id, ok := nm.Int("max_id")
	if !ok {
		t.Fatalf("integration: query_node node carries no max_id: %v", dumpMap(nm, ""))
	}
	return id
}

// rawUpload performs the two-frame upload turn by hand with caller-chosen max_id and
// sync_id, mirroring actions/upload.go's frame shapes exactly except for those two
// fields. The body is small, so it is written sequentially (the concurrent write the
// typed client uses exists for bodies large enough to hit socket backpressure).
func rawUpload(t *testing.T, sc *scenario, sess *transport.Session, parentID, name string, mtime, maxID, syncID int64, content []byte) *sproto.Map {
	t.Helper()
	hash := md4.ContentHash(content)
	params := func() *sproto.Map {
		return sproto.NewMap().
			Set("conflict_policy", sproto.String("compare_mtime")).
			Set("max_id", sproto.Int(maxID)).
			Set("mtime", sproto.NewMap().Set("mtime", sproto.Int(mtime)).Set("refer", sproto.Int(0))).
			Set("path", sproto.String("id:"+parentID+"/"+name)).
			Set("root_node_id", sproto.Int(sc.view.RootNodeID)).
			Set("root_node_path", sproto.String("/")).
			Set("sync_id", sproto.Int(syncID)).
			Set("file_type", sproto.String("file")).
			Set("is_dir", sproto.Int(0)).
			Set("exec_bit", sproto.NewMap().Set("exec_bit", sproto.Int(0)).Set("refer", sproto.Int(0)))
	}
	fileMeta := func(data []byte) *sproto.Map {
		m := sproto.NewMap().
			Set("hash", sproto.String(hash)).
			Set("is_delta", sproto.Int(0)).
			Set("real_size", sproto.Int(int64(len(content)))).
			Set("refer", sproto.Int(0)).
			Set("size", sproto.Int(int64(len(content))))
		if data != nil {
			m.Set("data", sproto.Binary{Data: data})
		}
		return m
	}

	head := sc.rawRequestFrom("upload", params()).
		Set("@proto", sproto.NewMap().
			Set("body-continue", sproto.Int(1)).
			Set("type", sproto.String("header")).
			Set("version", sproto.NewMap().Set("major", sproto.Int(7)).Set("minor", sproto.Int(0)))).
		Set("file", fileMeta(nil))
	body := sc.rawRequestFrom("upload", params()).
		Set("@proto", sproto.NewMap().
			Set("body-continue", sproto.Int(0)).
			Set("type", sproto.String("body"))).
		Set("file", fileMeta(content))

	return rawTurn(t, sess, head, body)
}

// rawRequest builds the common request envelope (action + session + identity) the way
// actions.buildRequest does, for a hand-built turn.
func (sc *scenario) rawRequest(action string) *sproto.Map {
	ci := sc.h.clientInfo()
	return sproto.NewMap().
		Set("action", sproto.String(action)).
		Set("session", sproto.String(sc.session)).
		Set("agent", ci.Agent(sc.deviceUUID)).
		Set("client_version", sproto.String(ci.ClientVersion())).
		Set("client_type", sproto.String(ci.Type)).
		Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(sc.dsID)))
}

// rawRequestFrom is rawRequest with a parameter map folded in.
func (sc *scenario) rawRequestFrom(action string, params *sproto.Map) *sproto.Map {
	req := sc.rawRequest(action)
	for _, k := range params.Keys() {
		v, _ := params.Get(k)
		req.Set(k, v)
	}
	return req
}

// rawTurn writes one (or two, for a body-continue exchange) request frames and returns
// the decoded reply, stepping over any unsolicited keep_alive frame the server
// interleaves (a reply read that takes one as its answer decodes cleanly, carries no
// verdict, and poisons the connection - see the keep_alive invariant).
func rawTurn(t *testing.T, sess *transport.Session, head, body *sproto.Map) *sproto.Map {
	t.Helper()
	if err := sess.Conn().WriteRequest(sproto.CmdAction, head); err != nil {
		t.Fatalf("integration: write the raw request: %v", err)
	}
	if body != nil {
		if err := sproto.NewEncoder(sess.Conn().NetConn()).Encode(body); err != nil {
			t.Fatalf("integration: write the raw body frame: %v", err)
		}
	}
	for range 8 {
		resp, oversize, err := sess.Conn().ReadCappedResponse(1 << 20)
		if err != nil {
			t.Fatalf("integration: read the raw reply: %v", err)
		}
		if oversize {
			t.Fatalf("integration: the raw reply carried an oversized inline binary")
		}
		m, ok := resp.(*sproto.Map)
		if !ok {
			t.Fatalf("integration: the raw reply decoded as %T, want a map", resp)
		}
		if typ, ok := m.String("type"); ok && typ == "keep_alive" {
			continue
		}
		return m
	}
	t.Fatalf("integration: the server sent only keep_alive frames")
	return nil
}

// replyErrno reads a reply's verdict in both shapes this server uses: the flat
// `errno` and the nested `error{code}` a live DSM actually sends.
func replyErrno(reply *sproto.Map) (int64, bool) {
	if code, ok := reply.Int("errno"); ok {
		return code, true
	}
	if raw, ok := reply.Get("error"); ok {
		if em, ok := raw.(*sproto.Map); ok {
			if code, ok := em.Int("code"); ok {
				return code, true
			}
		}
	}
	return 0, false
}
