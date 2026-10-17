//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/sproto"
)

// TestIntegration_LongPollRejectionRaw answers the half of the long_poll rejection residual
// that the typed client structurally cannot: when a live DSM rejects a `long_poll`
// subscribe because one member view is unacceptable, does the reply NAME the culprit?
//
// It matters for a design choice, not for safety. engine/pollquarantine.go attributes a
// shared-subscribe rejection by BISECTING - after a streak it re-subscribes to each
// member alone and quarantines only the view the server rejects on its own evidence.
// That is correct under every answer, but it costs a probe round per member. If the
// rejection reply carries the offending view id, direct attribution is strictly better
// and the probe round can go.
//
// `actions.Client` decodes `long_poll` down to the changed view ids, so the question
// needs a RAW turn: the request is built exactly as actions.buildRequest +
// Subscription.pobject build it (the shapes are wire facts this client already speaks),
// written over the session's own `sproto.Conn`, and the reply is dumped key by key -
// including nested maps, since a culprit id could ride inside an error object.
//
// Nothing is asserted about the answer: it is a measurement either way, and it changes
// a cost, not a correctness property.
func TestIntegration_LongPollRejectionRaw(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sess, err := sc.connector(ctx)
	if err != nil {
		t.Fatalf("integration: dial a session for the raw turn: %v", err)
	}
	defer func() { _ = sess.Close() }()

	subscription := func(viewID, nodeID int64) *sproto.Map {
		return sproto.NewMap().
			Set("sync_id", sproto.Int(0)).
			Set("path", sproto.String("/")).
			Set("view_id", sproto.Int(viewID)).
			Set("node_id", sproto.Int(nodeID)).
			Set("lock_id", sproto.Int(0)).
			Set("option_change_lock_id", sproto.Int(0)).
			Set("recursive", sproto.Int(1))
	}

	const bogusViewID = 999999
	ci := sc.h.clientInfo()
	req := sproto.NewMap().
		Set("action", sproto.String("long_poll")).
		Set("session", sproto.String(sc.session)).
		Set("agent", ci.Agent(sc.deviceUUID)).
		Set("client_version", sproto.String(ci.ClientVersion())).
		Set("client_type", sproto.String(ci.Type)).
		Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(sc.dsID))).
		Set("subscribe", sproto.Array{
			subscription(sc.view.ViewID, sc.view.RootNodeID),
			subscription(bogusViewID, bogusViewID),
		}).
		Set("timeout", sproto.Int(actions.MinPollTimeout))

	if err := sess.Conn().WriteRequest(sproto.CmdAction, req); err != nil {
		t.Fatalf("integration: write the raw long_poll: %v", err)
	}
	resp, oversize, err := sess.Conn().ReadCappedResponse(1 << 20)
	if err != nil {
		t.Fatalf("integration: read the raw long_poll reply: %v", err)
	}
	if oversize {
		t.Fatalf("integration: the raw reply carried an oversized inline binary")
	}
	reply, ok := resp.(*sproto.Map)
	if !ok {
		t.Fatalf("integration: the raw reply decoded as %T, want a map", resp)
	}

	t.Logf("RAW long_poll reply to a subscription of view %d + bogus view %d:",
		sc.view.ViewID, bogusViewID)
	for _, line := range dumpMap(reply, "  ") {
		t.Log(line)
	}

	// Does anything in the reply carry the bogus id? That is the whole question.
	flat := strings.Join(dumpMap(reply, ""), "\n")
	if strings.Contains(flat, fmt.Sprint(bogusViewID)) {
		t.Logf("VERDICT: the rejection NAMES the offending view (%d appears in the reply) - "+
			"direct attribution is available and engine/pollquarantine.go's bisect probe round "+
			"could be replaced by reading it. Record the exact key in DESIGN §13 first.",
			bogusViewID)
		return
	}
	t.Logf("VERDICT: the rejection does NOT name the offending view - nothing in the reply "+
		"carries %d. The bisect probe round in engine/pollquarantine.go is the only way to "+
		"attribute such a rejection, so it stays.", bogusViewID)
}

// dumpMap renders a decoded sproto map as sorted key: value lines, descending into
// nested maps and arrays so a culprit id nested inside an error object is visible.
func dumpMap(m *sproto.Map, indent string) []string {
	keys := append([]string(nil), m.Keys()...)
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		v, _ := m.Get(k)
		out = append(out, dumpValue(indent, k, v)...)
	}
	return out
}

// dumpValue renders one key/value pair, recursing into containers.
func dumpValue(indent, key string, v sproto.Value) []string {
	switch t := v.(type) {
	case *sproto.Map:
		out := []string{indent + key + ":"}
		return append(out, dumpMap(t, indent+"  ")...)
	case sproto.Array:
		out := []string{fmt.Sprintf("%s%s: [%d]", indent, key, len(t))}
		for i, e := range t {
			out = append(out, dumpValue(indent+"  ", fmt.Sprintf("#%d", i), e)...)
		}
		return out
	default:
		return []string{fmt.Sprintf("%s%s: %v", indent, key, v)}
	}
}
