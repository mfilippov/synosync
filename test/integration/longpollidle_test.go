//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
)

// TestIntegration_LongPollBlocksWhileIdle pins what makes long_poll LONG on a live DSM. A poll
// that does not send back the profile_digest of the previous reply, or whose subscription carries
// lock_id 0, is answered at once with the view flagged - on every poll, with nothing to pull. The
// engine spun on that: about twelve polls a second and a full local scan every few seconds on an
// idle daemon, measured on a 184k-file NAS as most of a CPU core. Here the second poll, carrying
// the digest the first one returned, must hold until something changes, and a change on the
// server must still wake it.
func TestIntegration_LongPollBlocksWhileIdle(t *testing.T) {
	sc := setupScenarioNoEngine(t)
	ctx, cancel := actionCtx()
	defer cancel()
	cursor, err := sc.client.MaxSyncID(ctx, sc.view.ViewID, sc.view.RootNodeID)
	if err != nil {
		t.Fatalf("integration: get_max_sync_id: %v", err)
	}
	subs := []actions.Subscription{{ViewID: sc.view.ViewID, NodeID: sc.view.RootNodeID, SyncID: cursor}}

	// The poll runs on its own connection, as the engine's event connection does, so the change
	// below can be made on sc.client while it is held.
	pc, closePC := newRemoteClient(t, sc.h, sc.connector, sc.session, sc.deviceUUID, sc.dsID)
	defer closePC()
	// The first poll of a client that has no digest yet is answered at once with one.
	fctx, fcancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err = pc.LongPoll(fctx, subs, actions.MinPollTimeout)
	fcancel()
	if err != nil {
		t.Fatalf("integration: first long_poll: %v", err)
	}
	if pc.ProfileDigest() == "" {
		t.Log("note: the first long_poll reply carried no profile_digest")
	}

	type result struct {
		changed []int64
		err     error
		at      time.Duration
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer pcancel()
		changed, err := pc.LongPoll(pctx, subs, actions.MinPollTimeout)
		done <- result{changed, err, time.Since(start)}
	}()

	const hold = 10 * time.Second
	select {
	case r := <-done:
		t.Fatalf("an idle long_poll returned after %s (changed=%v err=%v); it must hold until something changes",
			r.at.Round(time.Millisecond), r.changed, r.err)
	case <-time.After(hold):
	}

	changedAt := time.Since(start)
	parent := serverMkdir(t, sc.client, sc.view.RootNodeID, sc.viewRootID(t), sc.base)
	serverPut(t, sc.client, sc.view.RootNodeID, parent.FileID, "wake.txt", []byte("wake"))
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("the held long_poll failed: %v", r.err)
		}
		if len(r.changed) == 0 {
			t.Fatalf("the held long_poll returned at %s without flagging the view", r.at.Round(time.Millisecond))
		}
		t.Logf("held %s, then woke %s after the change began", hold, (r.at - changedAt).Round(time.Millisecond))
	case <-time.After(time.Minute):
		t.Fatal("a change on the server did not wake the held long_poll within a minute")
	}
	teardownRemote(t, sc.client, sc.view.ViewID, sc.base)
}
