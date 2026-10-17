package actions

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/sproto"
)

func TestRename(t *testing.T) {
	var req *sproto.Map
	c := renameMock(t, "sess", 100, func(t *testing.T, r *sproto.Map) sproto.Value {
		req = r
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})

	if err := c.Rename(testContext(t), 7, 1, "/dir/old.txt", "/other/new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if v, _ := req.Get("action"); v != sproto.String("rename") {
		t.Errorf("action = %v, want rename", v)
	}
	if v, _ := req.Get("session"); v != sproto.String("sess") {
		t.Errorf("session = %v, want sess", v)
	}
	if v, _ := req.Get("view_id"); v != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", v)
	}
	if v, _ := req.Get("src_path"); v != sproto.String("/dir/old.txt") {
		t.Errorf("src_path = %v, want /dir/old.txt", v)
	}
	if v, _ := req.Get("dst_path"); v != sproto.String("/other/new.txt") {
		t.Errorf("dst_path = %v, want /other/new.txt", v)
	}
}

func TestRenamePathError(t *testing.T) {
	c := renameMock(t, "sess", 100, func(t *testing.T, r *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	err := c.Rename(testContext(t), 7, 1, "/missing", "/dest")
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

func TestRemove(t *testing.T) {
	var req *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		req = r
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})

	if err := c.Remove(testContext(t), 7, "/dir/gone.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if v, _ := req.Get("action"); v != sproto.String("remove") {
		t.Errorf("action = %v, want remove", v)
	}
	if v, _ := req.Get("session"); v != sproto.String("sess") {
		t.Errorf("session = %v, want sess", v)
	}
	if v, _ := req.Get("view_id"); v != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", v)
	}
	if v, _ := req.Get("path"); v != sproto.String("/dir/gone.txt") {
		t.Errorf("path = %v, want /dir/gone.txt", v)
	}
	// Remove carries no destination path.
	if _, ok := req.Get("to_path"); ok {
		t.Errorf("remove request unexpectedly carries to_path")
	}
}

func TestRemovePathError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	err := c.Remove(testContext(t), 7, "/missing")
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

// TestMutateMissingErrnoRejected pins the doMutate strictness: a mutating
// action's reply that carries no errno at all must be rejected as
// ErrUnexpectedReply rather than read as a confirmed mutation - the reply's
// errno == 0 is the only positive evidence these actions get. The whole reply
// was read, so the connection stays byte-aligned and the Client is not poisoned.
func TestMutateMissingErrnoRejected(t *testing.T) {
	cases := []struct {
		name string
		call func(c *Client) error
	}{
		{"rename", func(c *Client) error { return c.Rename(testContext(t), 7, 1, "/old", "/new") }},
		{"remove", func(c *Client) error { return c.Remove(testContext(t), 7, "/gone") }},
		{"unlink_connection", func(c *Client) error { return c.UnlinkConnection(testContext(t)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
				// A half-formed reply map without errno.
				return sproto.NewMap().Set("data", sproto.String("ok"))
			})
			err := tc.call(c)
			if !errors.Is(err, ErrUnexpectedReply) {
				t.Fatalf("err = %v, want ErrUnexpectedReply", err)
			}
			if c.Broken() {
				t.Error("connection marked broken after a fully-read no-errno reply")
			}
		})
	}
}

// TestRemoveWireShape pins the exact request shape a live DSM requires. It is a
// LOAD-BEARING test, not a formality: the server answers 40961 both for an absent
// target and for a malformed request, and errors.go maps that code to ErrPathError
// ("already gone"). If the shape regresses - a path without the leading slash, or a
// zero sync_id sent alongside the scoping block - the server refuses the delete and
// the engine reads the refusal as "the node is already gone", acking a deletion that
// never happened.
func TestRemoveWireShape(t *testing.T) {
	var got *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	if err := c.Remove(testContext(t), 7, "dir/file.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if v, _ := got.Get("path"); v != sproto.String("/dir/file.txt") {
		t.Errorf("path = %v, want the LEADING-SLASH form /dir/file.txt", v)
	}
	if v, _ := got.Get("view_id"); v != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", v)
	}
	// The cursor block must be ABSENT: a live DSM refuses a mutating request that
	// carries any cursor (40962/40961) and applies nothing.
	for _, k := range []string{"max_id", "sync_id", "root_node_id", "root_node_path"} {
		if _, ok := got.Get(k); ok {
			t.Errorf("request carried %q; every cursor value is refused by the server", k)
		}
	}
}

// TestRenameWireShape pins rename's field NAMES: the server reads src_path/dst_path,
// not path/to_path, and both in the leading-slash form.
func TestRenameWireShape(t *testing.T) {
	var got *sproto.Map
	c := renameMock(t, "sess", 100, func(t *testing.T, req *sproto.Map) sproto.Value {
		got = req
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	if err := c.Rename(testContext(t), 1, 1, "a/old.txt", "b/new.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if v, _ := got.Get("src_path"); v != sproto.String("/a/old.txt") {
		t.Errorf("src_path = %v, want /a/old.txt", v)
	}
	if v, _ := got.Get("dst_path"); v != sproto.String("/b/new.txt") {
		t.Errorf("dst_path = %v, want /b/new.txt", v)
	}
	for _, k := range []string{"path", "to_path"} {
		if _, ok := got.Get(k); ok {
			t.Errorf("request carried the obsolete key %q", k)
		}
	}
}
