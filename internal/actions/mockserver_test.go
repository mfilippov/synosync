package actions

// Reusable in-memory mock sproto server for the actions tests. It serves over one
// end of a net.Pipe; the client end is wrapped in a *sproto.Conn (and a *Client).
// serveMock gives a test full raw control over the server side so it can script
// multi-frame exchanges (e.g. the upload body frame); turnHandler is the common
// single CmdAction turn (read request → assert → write bare reply). No real
// network is used.

import (
	"net"
	"testing"

	"github.com/mfilippov/synosync/internal/sproto"
)

// serveMock starts a mock server on one end of a net.Pipe and returns the client
// end as a *sproto.Conn. The serve callback runs on a server goroutine with the
// raw server-side connection and owns all reads and writes. The connection is
// closed and the goroutine joined at test cleanup.
func serveMock(t *testing.T, serve func(t *testing.T, server net.Conn)) *sproto.Conn {
	t.Helper()
	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		serve(t, server)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return sproto.NewConn(client)
}

// turnHandler handles one CmdAction turn: it receives the decoded request Map and
// returns the reply Value to encode back as a bare PObject. Returning nil writes
// no reply (used to exercise read failures).
type turnHandler func(t *testing.T, req *sproto.Map) sproto.Value

// singleTurn adapts a turnHandler into a serve function: it reads one request
// frame, asserts the command is CmdAction, hands the decoded request to handle,
// and writes handle's return value (if non-nil) as the bare reply.
func singleTurn(handle turnHandler) func(t *testing.T, server net.Conn) {
	return func(t *testing.T, server net.Conn) {
		h, err := sproto.ReadHeader(server)
		if err != nil {
			return
		}
		if h.Cmd != sproto.CmdAction {
			t.Errorf("request cmd = 0x%02x, want CmdAction (0x%02x)", h.Cmd, sproto.CmdAction)
		}
		dec := sproto.NewDecoder(server)
		v, err := dec.Decode()
		if err != nil {
			return
		}
		m, _ := v.(*sproto.Map)
		// Honour the two-frame contract: a request whose @proto announces
		// body-continue:1 is followed by a bare-PObject continuation frame, which the
		// server must consume before replying. Upload and Mkdir both use it; replying
		// after frame 1 alone would close the connection under the client's body
		// writer.
		if m != nil {
			if raw, ok := m.Get("@proto"); ok {
				if pm, ok := raw.(*sproto.Map); ok {
					if bc, ok := pm.Int("body-continue"); ok && bc == 1 {
						_, _ = dec.Decode()
					}
				}
			}
		}
		reply := handle(t, m)
		if reply == nil {
			return
		}
		_ = sproto.NewEncoder(server).Encode(reply)
	}
}

// mockClient wires a Client (with the given session token) to a mock server that
// answers a single CmdAction turn via handle. It is the convenience path for the
// many tests that need exactly one request/reply.
func mockClient(t *testing.T, session string, handle turnHandler) *Client {
	t.Helper()
	return newClient(serveMock(t, singleTurn(handle)), session)
}

// renameMock wires a Client to a server that answers the query_node turn Rename
// issues first, then hands the rename request itself to handle. Rename is a
// TWO-turn action: it reads the SOURCE NODE's own cursor (node.max_id), because the
// server rejects a rename whose cursor is below it with 40962 "Object has been
// modified". A single-turn mock cannot serve it.
func renameMock(t *testing.T, session string, maxSyncID int64, handle turnHandler) *Client {
	t.Helper()
	return newClient(serveMock(t, func(t *testing.T, server net.Conn) {
		for {
			h, err := sproto.ReadHeader(server)
			if err != nil {
				return
			}
			if h.Cmd != sproto.CmdAction {
				t.Errorf("request cmd = 0x%02x, want CmdAction", h.Cmd)
			}
			v, err := sproto.NewDecoder(server).Decode()
			if err != nil {
				return
			}
			m, _ := v.(*sproto.Map)
			var reply sproto.Value
			if act, _ := m.String("action"); act == "query_node" {
				reply = sproto.NewMap().
					Set("errno", sproto.Int(CodeOK)).
					Set("node", sproto.NewMap().Set("max_id", sproto.Int(maxSyncID)))
			} else {
				reply = handle(t, m)
			}
			if reply == nil {
				return
			}
			if err := sproto.NewEncoder(server).Encode(reply); err != nil {
				return
			}
		}
	}), session)
}
