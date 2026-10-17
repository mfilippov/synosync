//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
)

// TestIntegration_UnlinkConnection proves that `unlink_connection` actually RELEASES the
// device registration, rather than merely returning success.
//
// It matters because the harness's own stand hygiene now rests on it: TestMain unlinks the
// two devices each run pairs (unlinkPairedDevices), which is what stops a long-lived test
// DSM accumulating a pair of dead clients per run. A verb that answered OK while leaving the
// registration alive would make that cleanup a comfortable illusion, and the accumulation
// would go unnoticed precisely because the log says it was handled.
//
// The proof is behavioural, not cosmetic: a throwaway device is paired, shown to work
// (one authenticated turn), unlinked, and then asked to work again with the same token. The
// server must refuse it with 8193, the session-vanished verdict the client maps to
// auth.ErrSessionVanished - and measured, it refuses at CONNECT: mode-A reuse presents the
// token during the handshake, so a released registration is rejected a round trip before any
// action is sent. Either layer answering 8193 is the proof; anything still succeeding means
// the token outlived the unlink and the registration was not released.
func TestIntegration_UnlinkConnection(t *testing.T) {
	h := requireEnv(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*netTimeout)
	defer cancel()

	// A device of its own, so nothing here can disturb the cached engine/mutator pairings
	// the other scenarios share.
	p, err := pairDevice(ctx, h, "")
	if err != nil {
		t.Fatalf("integration: pair throwaway device: %v", err)
	}
	t.Logf("paired throwaway device %s", p.deviceUUID)

	// useToken dials a session with the pairing's token and runs one authenticated turn,
	// reporting whichever layer refuses first. query_user_info is the cheapest turn that
	// proves the session is live.
	useToken := func() error {
		sess, derr := p.connector(ctx)
		if derr != nil {
			return derr
		}
		defer func() { _ = sess.Close() }()
		ci := h.clientInfo()
		c := actions.NewClientWithIdentity(sess, p.session, actions.Identity{
			Agent:         ci.Agent(p.deviceUUID),
			ClientVersion: ci.ClientVersion(),
			ClientType:    ci.Type,
			DsID:          p.dsID,
		}, nil)
		if _, qerr := c.QueryUserInfo(ctx); qerr != nil {
			return qerr
		}
		return c.UnlinkConnection(ctx)
	}

	// The token works before the unlink - and this call performs it.
	if err := useToken(); err != nil {
		t.Fatalf("integration: the freshly paired session could not query and unlink: %v", err)
	}
	t.Logf("unlinked device %s", p.deviceUUID)

	// And must not work after it.
	err = useToken()
	switch {
	case err == nil:
		t.Errorf("integration: the token STILL WORKS after unlink_connection - it outlived the "+
			"unlink, so the harness's TestMain cleanup does not actually release device %s and "+
			"a stand keeps accumulating registrations", p.deviceUUID)
	case errors.Is(err, auth.ErrSessionVanished):
		t.Logf("VERDICT: after unlink the token is refused with the session-vanished verdict "+
			"(8193): %v", err)
	default:
		// Still a refusal, just not the code the client maps: worth naming, because the
		// engine's terminal handling keys on 8193 specifically.
		t.Logf("VERDICT: after unlink the token is refused, but NOT as 8193: %s", describeServerErr(err))
	}
}
