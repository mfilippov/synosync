package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
)

// TestProtocolStampedOnEveryReply reads the raw bytes off the socket, because the point
// is what goes on the WIRE: a client cannot tell "an old daemon" from "a daemon that
// failed this call" unless every reply carries the version, including the ones that never
// reach a handler. Stamping is done once at the writer for that reason, and this covers
// the success reply, a handler error and an unknown method through the real server.
func TestProtocolStampedOnEveryReply(t *testing.T) {
	sock := serveTest(t, func(s *Server) {
		s.Handle("ok", func(context.Context, json.RawMessage) (any, error) { return "fine", nil })
		s.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
			return nil, errors.New("handler said no")
		})
	})

	for _, method := range []string{"ok", "boom", "no-such-method"} {
		t.Run(method, func(t *testing.T) {
			// The package.s own Dial, so the test works over a unix socket and a named
			// pipe alike; the raw connection under it is what the assertion needs.
			c, err := Dial(sock)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Close()
			conn := c.conn
			if err := json.NewEncoder(conn).Encode(request{Protocol: Protocol, Method: method}); err != nil {
				t.Fatalf("write request: %v", err)
			}
			var raw map[string]json.RawMessage
			if err := json.NewDecoder(conn).Decode(&raw); err != nil {
				t.Fatalf("read reply: %v", err)
			}
			got, ok := raw["protocol"]
			if !ok {
				t.Fatalf("reply carries no protocol field: %v", raw)
			}
			if string(got) != strconv.Itoa(Protocol) {
				t.Fatalf("reply protocol = %s, want %d", got, Protocol)
			}
		})
	}
}

// TestServerRefusesANewerCaller: a caller from a newer build may mean something else by a
// method name this daemon also has, so the version is checked before the method is looked
// up. This is the direction that works TODAY for a new CLI against an old daemon, which
// is exactly what a package upgrade leaves behind until the service restarts.
func TestServerRefusesANewerCaller(t *testing.T) {
	srv := NewServer()
	called := false
	srv.Handle("status", func(context.Context, json.RawMessage) (any, error) {
		called = true
		return nil, nil
	})

	resp := srv.dispatch(context.Background(), request{Protocol: Protocol + 1, Method: "status"})
	if called {
		t.Fatal("the handler ran for a caller speaking a protocol this build does not know")
	}
	if !strings.Contains(resp.Error, "newer control protocol") {
		t.Fatalf("reply error = %q, want a protocol mismatch", resp.Error)
	}
	if !strings.Contains(resp.Error, "restart the daemon") {
		t.Fatalf("reply error = %q, want it to say what to do", resp.Error)
	}
}

// TestServerAnswersAPreVersioningCaller: a client that predates the field sends no
// protocol at all. Nothing has changed meaning, so refusing it would break a working
// setup for nothing.
func TestServerAnswersAPreVersioningCaller(t *testing.T) {
	srv := NewServer()
	srv.Handle("status", func(context.Context, json.RawMessage) (any, error) { return "fine", nil })

	resp := srv.dispatch(context.Background(), request{Method: "status"})
	if resp.Error != "" {
		t.Fatalf("reply error = %q, want a pre-versioning caller answered normally", resp.Error)
	}
}

// TestProtocolMismatchErrorNamesBothVersions: the message exists to end a support
// conversation, so it has to carry the two numbers and the action.
func TestProtocolMismatchErrorNamesBothVersions(t *testing.T) {
	err := &ProtocolMismatchError{Peer: 9, Local: 2, Role: "daemon"}
	msg := err.Error()
	for _, want := range []string{"daemon", "v9", "v2", "restart the daemon"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q is missing %q", msg, want)
		}
	}
}

// TestClientRefusesANewerDaemon covers the other direction: a daemon that speaks a
// protocol this build does not know may mean anything by the fields it sent, including
// ones this build recognises by name, so the reply must not be interpreted at all.
func TestClientRefusesANewerDaemon(t *testing.T) {
	// A reply carrying a plausible result AND a newer protocol: the result must not be
	// decoded, since this build cannot know what its fields now mean.
	resp := response{Protocol: Protocol + 1, Result: json.RawMessage(`{"server":"nas:6690"}`)}
	var mismatch *ProtocolMismatchError
	err := clientProtocolCheck(resp)
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v, want a ProtocolMismatchError", err)
	}
	if mismatch.Role != "daemon" {
		t.Fatalf("role = %q, want the daemon named as the newer end", mismatch.Role)
	}
	if !strings.Contains(mismatch.Error(), "restart the daemon") {
		t.Fatalf("message = %q, want it to say what to do", mismatch.Error())
	}
}
