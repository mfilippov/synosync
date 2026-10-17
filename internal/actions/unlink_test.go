package actions

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/sproto"
)

func TestUnlinkConnection(t *testing.T) {
	var req *sproto.Map
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		req = r
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})

	if err := c.UnlinkConnection(testContext(t)); err != nil {
		t.Fatalf("UnlinkConnection: %v", err)
	}
	if v, _ := req.Get("action"); v != sproto.String("unlink_connection") {
		t.Errorf("action = %v, want unlink_connection", v)
	}
	if v, _ := req.Get("session"); v != sproto.String("sess") {
		t.Errorf("session = %v, want sess", v)
	}
	// The detach request carries no parameters of its own.
	for _, k := range req.Keys() {
		if k != "action" && k != "session" {
			t.Errorf("unexpected request key %q", k)
		}
	}
}

func TestUnlinkConnectionSessionVanished(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, r *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeSessionVanished))
	})
	err := c.UnlinkConnection(testContext(t))
	if !errors.Is(err, auth.ErrSessionVanished) {
		t.Fatalf("err = %v, want auth.ErrSessionVanished", err)
	}
}
